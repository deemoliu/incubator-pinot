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
package org.apache.pinot.query.runtime.operator.operands;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public class ReferenceOperand implements TransformOperand {
  private final int _index;
  private final ColumnDataType _resultType;

  public ReferenceOperand(int index, DataSchema dataSchema) {
    _index = index;
    _resultType = dataSchema.getColumnDataType(index);
  }

  @Override
  public ColumnDataType getResultType() {
    return _resultType;
  }

  @Nullable
  @Override
  public Object apply(List<Object> row) {
    return row.get(_index);
  }
}
