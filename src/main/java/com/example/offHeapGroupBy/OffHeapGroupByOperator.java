package com.example.offHeapGroupBy;

import com.bits.heap.OffHeapMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffHeapGroupByOperator extends BaseOperator{

    private static final Logger LOG = LoggerFactory.getLogger(OffHeapGroupByOperator.class);

    public final transient DefaultOutputPort<TransactionSchema> output = new DefaultOutputPort<>();

    public final transient DefaultInputPort<TransactionSchema> input = new DefaultInputPort<TransactionSchema>() {
        @Override
        public void process(TransactionSchema tuple) {
            groupBy(tuple);
        }
    };

    public final int MAX = 20000000;

    private OffHeapMap offHeapMap;

    public OffHeapGroupByOperator() {}

    void groupBy(TransactionSchema tuple) {
        if(tuple.getId() == 1)
            LOG.info("START 1 : " + System.currentTimeMillis());
        tuple.setPrice(offHeapMap.put(tuple.getId(), tuple.getPrice()));
        output.emit(tuple);
        if(tuple.getId() == MAX) {
            LOG.info("DONE " + MAX + " : " + System.currentTimeMillis());
            throw new ShutdownException();
        }
    }

    @Override
    public void setup(Context.OperatorContext context) { offHeapMap = new OffHeapMap(); }

    @Override
    public void endWindow() { LOG.info("data size : " + offHeapMap.size());}



}
