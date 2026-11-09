<p align="center">
  <img src="https://raw.githubusercontent.com/JorgeCostaMacia/bus-kafka-dotnet/main/assets/social-preview.png" width="100%" alt="bus-kafka-dotnet" />
</p>

# bus-kafka-dotnet

> The Kafka transport of [JorgeCostaMacia.Bus](https://github.com/JorgeCostaMacia/bus-dotnet) — the command/event bus over Confluent.Kafka, with Quartz-backed delayed retry and health checks — each scoped to a single concern and shipped independently under `JorgeCostaMacia.Bus.Kafka.*`.

[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-kafka-dotnet.svg)](LICENSE.txt)
[![Main](https://github.com/JorgeCostaMacia/bus-kafka-dotnet/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-kafka-dotnet/actions/workflows/main.yml)
[![Develop](https://github.com/JorgeCostaMacia/bus-kafka-dotnet/actions/workflows/develop.yml/badge.svg?branch=develop)](https://github.com/JorgeCostaMacia/bus-kafka-dotnet/actions/workflows/develop.yml)

A MassTransit-free bus: the contracts live in [bus-dotnet](https://github.com/JorgeCostaMacia/bus-dotnet), the other transport in [bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet), and each releases on its own. Part of the `JorgeCostaMacia.*` family, alongside [ddd-dotnet](https://github.com/JorgeCostaMacia/ddd-dotnet), [quartz-dotnet](https://github.com/JorgeCostaMacia/quartz-dotnet), [serilog-dotnet](https://github.com/JorgeCostaMacia/serilog-dotnet) and [http-dotnet](https://github.com/JorgeCostaMacia/http-dotnet). Moved out of [bus-net](https://github.com/JorgeCostaMacia/bus-net) in 5.0.0, keeping the package ids: 5.0.0 is a major because `Event` now implements the `IDomainEvent` of `JorgeCostaMacia.DDD.DomainEvent`.

## Requirements

The **.NET 10** SDK.

## Packages

| Package | What it does |
| --- | --- |
| [JorgeCostaMacia.Bus.Kafka](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka/) | Transport on `Confluent.Kafka` — commands (`Send`) and events (`Publish`). |
| [JorgeCostaMacia.Bus.Kafka.Retry.Quartz](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.Retry.Quartz/) | Quartz-backed delayed retry for the transport — parks a failed delivery and re-produces it at its time. |
| [JorgeCostaMacia.Bus.Kafka.HealthChecks](https://www.nuget.org/packages/JorgeCostaMacia.Bus.Kafka.HealthChecks/) | Health check for the transport — the bus's broker reachability on ASP.NET Core health endpoints. |

## Upgrading from 4.x (bus-net)

Bump the three packages together with `JorgeCostaMacia.DDD.DomainEvent` 11.0.0 (ddd-dotnet), and rename `JorgeCostaMacia.DomainEvent` to `JorgeCostaMacia.DDD.DomainEvent` in your `using`s. Since 5.1.0 the log messages are PascalCase event names (`Subscriber failed.` is `SubscriberFailed`), so a query or an alert on the old text needs the new one, and `.Retry.Quartz` is built on Quartz 4.7, whose store needs `triggers.fixed_delay` (Quartz's `database/migrations/4.7/add_trigger_fixed_delay`) before a 4.7 node starts the scheduler.

## Upgrading to 5.2.0

The consumer prefetches less by default: `QueuedMaxMessagesKbytes` is 2048 (it was 8192) and `QueuedMinMessages` 1000 (it was 5000), so a service with hundreds of consumers holds far less memory through a reload or a start with a backlog. A service that runs few consumers at high throughput sets them back in `Bus:Consumer`. The client's internal log message `Kafka log.` is `KafkaLog`, as the others.

## Contact

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
