using Confluent.Kafka;
using JorgeCostaMacia.Bus.Kafka.Infrastructure;
using JorgeCostaMacia.Bus.Kafka.Infrastructure.Kafka;
using JorgeCostaMacia.Bus.Kafka.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;

namespace JorgeCostaMacia.Bus.Kafka.Tests.Infrastructure.Kafka;

/// <summary>
/// The client-level callback reactions, invoked directly off the builder they were wired onto rather
/// than provoked out of librdkafka — so what is pinned is the reaction, deterministically: which errors
/// flip the readiness probe, which ones take the process down, and which ones the client is left to
/// recover from on its own. Both builder overloads are covered because they are separate wirings, and a
/// handler missing from one of them would be silent.
/// </summary>
public sealed class KafkaClientCallbacksTests
{
    [Fact]
    public void WithClientCallbacks_AnAllBrokersDownError_FlipsHealthDownButKeepsTheAppRunning()
    {
        // losing every broker is what the readiness probe exists to report; it is not fatal, so the
        // process must stay up and let the client reconnect.
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ProducerBuilderProbe probe = (ProducerBuilderProbe)Wire(new ProducerBuilderProbe(), health, lifetime);

        probe.Error!(null!, new Error(ErrorCode.Local_AllBrokersDown, "down", isFatal: false));

        Assert.False(health.IsUp);
        Assert.False(lifetime.StopRequested);
    }

    [Fact]
    public void WithClientCallbacks_AFatalError_StopsTheApplication()
    {
        // a fatal client never recovers, so running on would mean a process that is up and deaf.
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ProducerBuilderProbe probe = (ProducerBuilderProbe)Wire(new ProducerBuilderProbe(), health, lifetime);

        probe.Error!(null!, new Error(ErrorCode.Local_Fatal, "fatal", isFatal: true));

        Assert.True(lifetime.StopRequested);
    }

    [Fact]
    public void WithClientCallbacks_AnyOtherError_TouchesNeitherHealthNorTheApplication()
    {
        // the common case: a transient, non-fatal, single-broker error the client retries by itself.
        // Flipping readiness down here would make every blip look like an outage.
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ProducerBuilderProbe probe = (ProducerBuilderProbe)Wire(new ProducerBuilderProbe(), health, lifetime);

        probe.Error!(null!, new Error(ErrorCode.Local_TimedOut, "timeout", isFatal: false));

        Assert.True(health.IsUp);
        Assert.False(lifetime.StopRequested);
    }

    [Fact]
    public void WithClientCallbacks_AFatalAllBrokersDownError_FlipsHealthDownAndStops()
    {
        // the two reactions are independent ifs, not a chain, so an error that is both must do both.
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ProducerBuilderProbe probe = (ProducerBuilderProbe)Wire(new ProducerBuilderProbe(), health, lifetime);

        probe.Error!(null!, new Error(ErrorCode.Local_AllBrokersDown, "down for good", isFatal: true));

        Assert.False(health.IsUp);
        Assert.True(lifetime.StopRequested);
    }

    [Fact]
    public void WithClientCallbacks_OnAProducerBuilder_SetsTheErrorLogAndStatisticsHandlers()
    {
        ProducerBuilderProbe probe = (ProducerBuilderProbe)Wire(
            new ProducerBuilderProbe(),
            new BusHealth(),
            new LifetimeFake());

        Assert.NotNull(probe.Error);
        Assert.NotNull(probe.Log);
        Assert.NotNull(probe.Statistics);
    }

    [Fact]
    public void WithClientCallbacks_OnAConsumerBuilder_SetsTheErrorLogAndStatisticsHandlers()
    {
        // the two overloads are hand-written twins (the builders share no base exposing the setters),
        // so the consumer's wiring is asserted separately: dropping a handler from one is invisible.
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ConsumerBuilderProbe probe = new ConsumerBuilderProbe();

        probe.WithClientCallbacks(NullLogger<KafkaClientCallbacksTests>.Instance, health, lifetime);

        Assert.NotNull(probe.Error);
        Assert.NotNull(probe.Log);
        Assert.NotNull(probe.Statistics);
    }

    [Fact]
    public void WithClientCallbacks_AFatalAllBrokersDownOnAConsumer_FlipsHealthDownAndStops()
    {
        BusHealth health = new BusHealth();
        LifetimeFake lifetime = new LifetimeFake();
        ConsumerBuilderProbe probe = new ConsumerBuilderProbe();
        probe.WithClientCallbacks(NullLogger<KafkaClientCallbacksTests>.Instance, health, lifetime);

        probe.Error!(null!, new Error(ErrorCode.Local_AllBrokersDown, "down", isFatal: true));

        Assert.False(health.IsUp);
        Assert.True(lifetime.StopRequested);
    }

    [Fact]
    public void WithClientCallbacks_ALogAndAStatisticsEvent_PassesBothToTheClientLogger()
    {
        // they are pure passthroughs, and the logger is the only place their effect shows.
        RecordingLogger<KafkaClientCallbacksTests> logger = new RecordingLogger<KafkaClientCallbacksTests>();
        ProducerBuilderProbe probe = new ProducerBuilderProbe();
        probe.WithClientCallbacks(logger, new BusHealth(), new LifetimeFake());

        probe.Log!(null!, new LogMessage("client", SyslogLevel.Info, "facility", "internal"));
        probe.Statistics!(null!, "{}");

        Assert.Equal(
            new List<string>() { "Kafka log.", "KafkaStatistics" },
            logger.Logged.Select(entry => entry.Message).ToList());
    }

    /// <summary>Wires the callbacks onto a producer builder and hands the same instance back as the probe.</summary>
    /// <param name="probe">The probe builder.</param>
    /// <param name="health">The readiness tracker.</param>
    /// <param name="lifetime">The application lifetime.</param>
    /// <returns>The same builder.</returns>
    private static ProducerBuilder<Null, byte[]> Wire(
        ProducerBuilderProbe probe,
        BusHealth health,
        LifetimeFake lifetime
    ) => probe.WithClientCallbacks(NullLogger<KafkaClientCallbacksTests>.Instance, health, lifetime);
}
