/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0f (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0f
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.aggregators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.io.FloatWritable;
import org.junit.Test;

public class TestFloatAggregators {

  @Test
  public void testAverageAggregator() {
    FloatAverageAggregator max = new FloatAverageAggregator();
    max.aggregate(1.0f);
    max.aggregate(new FloatWritable(3.0f));
    assertEquals(2.0f, max.getAggregatedValue().get());
    max.resetAggregatedValue();
    assertEquals(0.0f, max.getAggregatedValue().get());
    FloatWritable fw = max.createAggregatedValue();
    assertNotNull(fw);
  }
  
  @Test
  public void testMaxAggregator() {
    FloatMaxAggregator max = new FloatMaxAggregator();
    max.aggregate(2.0f);
    max.aggregate(new FloatWritable(3.0f));
    assertEquals(3.0f, max.getAggregatedValue().get());
    max.setAggregatedValue(1.0f);
    assertEquals(1.0f, max.getAggregatedValue().get());
    FloatWritable fw = max.createAggregatedValue();
    assertNotNull(fw);
  }

  @Test
  public void testMinAggregator() {
    FloatMinAggregator min = new FloatMinAggregator();
    min.aggregate(3.0f);
    min.aggregate(new FloatWritable(2.0f));
    assertEquals(2.0f, min.getAggregatedValue().get());
    min.setAggregatedValue(3.0f);
    assertEquals(3.0f, min.getAggregatedValue().get());
    FloatWritable fw = min.createAggregatedValue();
    assertNotNull(fw);
  }

  @Test
  public void testOverwriteAggregator() {
    FloatOverwriteAggregator overwrite = new FloatOverwriteAggregator();
    overwrite.aggregate(1.0f);
    assertEquals(1.0f, overwrite.getAggregatedValue().get());
    overwrite.aggregate(new FloatWritable(2.0f));
    assertEquals(2.0f, overwrite.getAggregatedValue().get());
    overwrite.setAggregatedValue(3.0f);
    assertEquals(3.0f, overwrite.getAggregatedValue().get());
    FloatWritable fw = overwrite.createAggregatedValue();
    assertNotNull(fw);
  }
  
  @Test
  public void testProductAggregator() {
    FloatProductAggregator product = new FloatProductAggregator();
    product.aggregate(6.0f);
    product.aggregate(new FloatWritable(7.0f));
    assertEquals(42.0f, product.getAggregatedValue().get());
    product.setAggregatedValue(1.0f);
    assertEquals(1.0f, product.getAggregatedValue().get());
    FloatWritable fw = product.createAggregatedValue();
    assertNotNull(fw);
  }

  @Test
  public void testSumAggregator() {
    FloatSumAggregator sum = new FloatSumAggregator();
    sum.aggregate(1.0f);
    sum.aggregate(new FloatWritable(2.0f));
    assertEquals(3.0f, sum.getAggregatedValue().get());
    sum.setAggregatedValue(4.0f);
    assertEquals(4.0f, sum.getAggregatedValue().get());
    FloatWritable fw = sum.createAggregatedValue();
    assertNotNull(fw);
  }

}
