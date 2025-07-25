/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class contains the shared logic across all different exchange types for exchanging data across servers.
 */
public abstract class BlockExchange {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockExchange.class);
  // TODO: Deduct this value via grpc config maximum byte size; and make it configurable with override.
  // TODO: Max block size is a soft limit. only counts fixedSize datatable byte buffer
  private static final int MAX_MAILBOX_CONTENT_SIZE_BYTES = 4 * 1024 * 1024;

  private final List<SendingMailbox> _sendingMailboxes;
  private final BlockSplitter _splitter;
 private final Function<List<SendingMailbox>, Integer> _statsIndexChooser;

  protected static final Function<List<SendingMailbox>, Integer> RANDOM_INDEX_CHOOSER =
      (mailboxes) -> ThreadLocalRandom.current().nextInt(mailboxes.size());

  /**
   * Factory method to create a BlockExchange based on the distribution type.
   *
   * It is important to notice that stats should only be sent to one mailbox to avoid sending the same stats multiple
   * times.
   * The statsIndexChooser function is used to choose the mailbox index to send stats to.
   * In most cases the {@link #RANDOM_INDEX_CHOOSER} should be used, but in some cases, like when using spools, the
   * mailbox index that receives the stats should be tuned.
   * @param statsIndexChooser a function to choose the mailbox index to send stats to.
   */
  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes,
      RelDistribution.Type distributionType, List<Integer> keys, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser, String hashFunction) {
    switch (distributionType) {
      case SINGLETON:
        return new SingletonExchange(sendingMailboxes, splitter, statsIndexChooser);
      case HASH_DISTRIBUTED:
        return new HashExchange(sendingMailboxes, KeySelectorFactory.getKeySelector(keys, hashFunction), splitter,
            statsIndexChooser);
      case RANDOM_DISTRIBUTED:
        return new RandomExchange(sendingMailboxes, splitter, statsIndexChooser);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchange(sendingMailboxes, splitter, statsIndexChooser);
      case ROUND_ROBIN_DISTRIBUTED:
      case RANGE_DISTRIBUTED:
      case ANY:
      default:
        throw new UnsupportedOperationException("Unsupported distribution type: " + distributionType);
    }
  }

  public static BlockExchange getExchange(List<SendingMailbox> sendingMailboxes, RelDistribution.Type distributionType,
      List<Integer> keys, BlockSplitter splitter, String hashFunction) {
    return getExchange(sendingMailboxes, distributionType, keys, splitter, RANDOM_INDEX_CHOOSER, hashFunction);
  }

  protected BlockExchange(List<SendingMailbox> sendingMailboxes, BlockSplitter splitter,
      Function<List<SendingMailbox>, Integer> statsIndexChooser) {
    _sendingMailboxes = sendingMailboxes;
    _splitter = splitter;
    _statsIndexChooser = statsIndexChooser;
  }

  /**
   * API to send a block to the destination mailboxes.
   * @param block the block to be transferred
   * @return true if all the mailboxes has been early terminated.
   * @throws IOException when sending stream unexpectedly closed.
   * @throws TimeoutException when sending stream timeout.
   */
  public boolean send(MseBlock.Data block)
      throws IOException, TimeoutException {
    boolean isEarlyTerminated = true;
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      if (!sendingMailbox.isEarlyTerminated()) {
        isEarlyTerminated = false;
        break;
      }
    }
    if (!isEarlyTerminated) {
      route(_sendingMailboxes, block);
    }
    return isEarlyTerminated;
  }

  /**
   * API to send a block to the destination mailboxes.
   * @param eosBlock the block to be transferred
   * @return true if all the mailboxes has been early terminated.
   * @throws IOException when sending stream unexpectedly closed.
   * @throws TimeoutException when sending stream timeout.
   */
  public boolean send(MseBlock.Eos eosBlock, List<DataBuffer> serializedStats)
      throws IOException, TimeoutException {
    int numMailboxes = _sendingMailboxes.size();
    int mailboxIdToSendMetadata;
    if (!serializedStats.isEmpty()) {
      mailboxIdToSendMetadata = _statsIndexChooser.apply(_sendingMailboxes);
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Sending EOS metadata. Only mailbox #{} will get stats", mailboxIdToSendMetadata);
      }
    } else {
      LOGGER.trace("Sending empty EOS metadata. No stat will be sent");
      // this may happen when the block exchange is itself used as a sending mailbox, like when using spools
      mailboxIdToSendMetadata = -1;
    }
    for (int i = 0; i < numMailboxes; i++) {
      SendingMailbox sendingMailbox = _sendingMailboxes.get(i);
      List<DataBuffer> statsToSend = i == mailboxIdToSendMetadata ? serializedStats : Collections.emptyList();

      sendingMailbox.send(eosBlock, statsToSend);
      sendingMailbox.complete();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Block sent: {} {} to {}", eosBlock, System.identityHashCode(eosBlock), sendingMailbox);
      }
    }
    return false;
  }

  protected void sendBlock(SendingMailbox sendingMailbox, MseBlock.Data block)
      throws IOException, TimeoutException {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Sending block: {} {} to {}", block, System.identityHashCode(block), sendingMailbox);
    }

    if (sendingMailbox.isLocal()) {
      sendingMailbox.send(block);
    } else {
      Iterator<? extends MseBlock.Data> splits = _splitter.split(block, MAX_MAILBOX_CONTENT_SIZE_BYTES);
      while (splits.hasNext()) {
        sendingMailbox.send(splits.next());
      }
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Block sent: {} {} to {}", block, System.identityHashCode(block), sendingMailbox);
    }
  }

  protected abstract void route(List<SendingMailbox> destinations, MseBlock.Data block)
      throws IOException, TimeoutException;

  // Called when the OpChain gracefully returns.
  // TODO: This is a no-op right now.
  public void close() {
  }

  public void cancel(Throwable t) {
    for (SendingMailbox sendingMailbox : _sendingMailboxes) {
      sendingMailbox.cancel(t);
    }
  }

  public SendingMailbox asSendingMailbox(String id) {
    return new BlockExchangeSendingMailbox(id);
  }

  /**
   * A mailbox that sends data blocks to a {@link BlockExchange}.
   *
   * BlockExchanges send data to a list of {@link SendingMailbox}es, which are responsible for sending the data
   * to the corresponding {@link ReceivingMailbox}es. This class applies the decorator pattern to expose a BlockExchange
   * as a SendingMailbox, open the possibility of having a BlockExchange as a destination for another BlockExchange.
   *
   * This is useful for example when a send operator has to send data to more than one stage. We need to broadcast the
   * data to all the stages (the first BlockExchange). Then for each stage, we need to send the data to the
   * corresponding workers (the inner BlockExchange). The inner BlockExchange may send data using a different
   * distribution strategy.
   */
  private class BlockExchangeSendingMailbox implements SendingMailbox {
    private final String _id;
    private boolean _earlyTerminated = false;
    private boolean _completed = false;

    public BlockExchangeSendingMailbox(String id) {
      _id = id;
    }

    @Override
    public boolean isLocal() {
      return true;
    }

    @Override
    public void send(MseBlock.Data data)
        throws IOException, TimeoutException {
      sendPrivate(data, Collections.emptyList());
    }

    @Override
    public void send(MseBlock.Eos block, List<DataBuffer> serializedStats)
        throws IOException, TimeoutException {
      sendPrivate(block, serializedStats);
    }

    private void sendPrivate(MseBlock block, List<DataBuffer> serializedStats)
        throws IOException, TimeoutException {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Exchange mailbox {} echoing {} {}", this, block, System.identityHashCode(block));
      }
      if (block.isData()) {
        Preconditions.checkArgument(serializedStats.isEmpty(), "Data block cannot have stats");
        _earlyTerminated = BlockExchange.this.send(((MseBlock.Data) block));
      } else {
        _earlyTerminated = BlockExchange.this.send(((MseBlock.Eos) block), serializedStats);
      }
    }

    @Override
    public void complete() {
      _completed = true;
    }

    @Override
    public void cancel(Throwable t) {
      BlockExchange.this.cancel(t);
    }

    @Override
    public boolean isTerminated() {
      return _completed;
    }

    @Override
    public boolean isEarlyTerminated() {
      return _earlyTerminated;
    }

    @Override
    public String toString() {
      return "e" + _id;
    }
  }
}
