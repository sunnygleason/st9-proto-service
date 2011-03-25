package ptest.com.g414.st9.proto.service.perf;

import static com.sun.faban.driver.CycleType.THINKTIME;

import java.util.concurrent.TimeUnit;

import com.google.inject.Key;
import com.sun.faban.driver.BenchmarkDefinition;
import com.sun.faban.driver.BenchmarkDriver;
import com.sun.faban.driver.BenchmarkOperation;
import com.sun.faban.driver.FlatMix;
import com.sun.faban.driver.NegativeExponential;

@BenchmarkDefinition(name = "BasicDriverScenario00", version = "1.0")
@BenchmarkDriver(name = "BasicDriverScenario00", responseTimeUnit = TimeUnit.MICROSECONDS)
@FlatMix(operations = { "create" }, mix = { 1.0 })
public class BasicDriverScenario00 extends BasicDriverScenarioBase {
    public BasicDriverScenario00() throws Exception {
        super();
    }

    @BenchmarkOperation(name = "create", max90th = 100000)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void create() throws Exception {
        super.create();
    }

    public static class GuiceModule extends BasicDriverScenarioBase.GuiceModule {
        @Override
        protected void configure() {
            super.configure();

            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    BasicDriverScenario00.class);
        }
    }
}
