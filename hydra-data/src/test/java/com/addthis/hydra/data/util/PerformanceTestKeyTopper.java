package com.addthis.hydra.data.util;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class PerformanceTestKeyTopper {

    private static enum Distribution {
        UNIFORM, NORMAL, EXPONENTIAL
    }

    static long[] generateValues(int count, Distribution distribution) {
        long[] values = new long[count];
        RealDistribution generator;
        switch(distribution) {
            case UNIFORM:
                generator = new UniformRealDistribution();
                break;
            case NORMAL:
                generator = new NormalDistribution();
                break;
            case EXPONENTIAL:
                generator = new ExponentialDistribution(1.0);
                break;
            default:
                throw new IllegalStateException("Unrecognized distribution " + distribution);
        }
        for(int i = 0; i < count; i++) {
            values[i] = Math.round(generator.sample());
        }
        return values;
    }

    public static void iteration(int count, int topK, Distribution distribution, boolean print) {
        long startTime, endTime;
        long[] values = generateValues(count, distribution);
        ConcurrentKeyTopper ckeyTopper = new ConcurrentKeyTopper();
        ckeyTopper.init();
        startTime = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            ckeyTopper.increment(String.valueOf(values[i]), topK);
        }
        endTime = System.currentTimeMillis();
        if (print) System.out.println("ConcurrentKeyTopper" + "\t\t" + count +
                           "\t" + topK + "\t" + distribution +
                           "\t" + (endTime - startTime));
        AltKeyTopper keyTopper = new AltKeyTopper();
        startTime = System.currentTimeMillis();
        for(int i = 0; i < count; i++) {
            keyTopper.increment(String.valueOf(values[i]), topK);
        }
        endTime = System.currentTimeMillis();
        if (print) System.out.println("KeyTopper" + "\t\t\t\t" + count +
                           "\t" + topK + "\t" + distribution +
                           "\t" + (endTime - startTime));
    }

    public static void main(String[] args) {
        // JVM warmup
        System.out.println("Begin JVM warmup.");
        for(int i = 0; i < 100; i++) {
            iteration(100000, 100, Distribution.UNIFORM, false);
        }
        System.out.println("End JVM warmup.");
        iteration(100000000, 100, Distribution.UNIFORM, true);
        iteration(100000000, 100, Distribution.EXPONENTIAL, true);
        iteration(100000000, 100, Distribution.NORMAL, true);
    }

}
