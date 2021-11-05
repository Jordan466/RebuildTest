using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Dahomey.Json;
using Dahomey.Json.Serialization.Conventions;
using Dahomey.Json.Serialization.Converters.DictionaryKeys;
using Dahomey.Json.Serialization.Converters.Mappings;
using LamarCodeGeneration;
using Marten;
using Marten.Events;
using Marten.Events.Aggregation;
using Marten.Events.Projections;
using Marten.NodaTime;
using Marten.Services;
using Marten.Services.Json;
using Marten.Storage;
using Microsoft.Extensions.Logging;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;
using Serilog;
using Serilog.Exceptions;
using Serilog.Extensions.Logging;
using TupleAsJsonArray;

namespace Value
{
    public record Contact(string Name);
}

namespace Test
{
    public interface IEvent
    {
        Guid Id { get; }
    }
    public interface IEntity
    {
        Guid Id { get; }
    }
    public interface ICreateEvent : IEvent { }

    public record ContactCreated(Guid Id, Guid Site, Value.Contact Contact) : ICreateEvent;

    public record Contact(Guid Id, Guid Site, Value.Contact Value) : IEntity
    {
        public static Contact? Create(ICreateEvent ev) => ev switch
        {
            ContactCreated e => new(e.Id, e.Site, e.Contact),
            _ => null
        };
    }

    public class ContactProjection : AggregateProjection<Contact>
    {
        public ContactProjection()
        {
            ProjectionName = nameof(Contact);
            Lifecycle = ProjectionLifecycle.Inline;

            CreateEvent<ICreateEvent>(Contact.Create!);
        }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var database = "test";
            var password = "Password12!";
            var conn = $"User ID=postgres;Password={password};Host=localhost;Port=5432;Database={database};";
            var store = DocumentStore.For(_ => ConfigureMarten(_, conn));
            await using var session = store.OpenSession(Guid.NewGuid().ToString());
            var @event = new ContactCreated(Guid.NewGuid(), Guid.NewGuid(), new Value.Contact("Test"));
            session.Events.StartStream(@event.Id, @event);
            session.SaveChanges();

            var logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .WriteTo.Async(a => a.Console());
            var microsoftLogger = new SerilogLoggerFactory(logger.CreateLogger()).CreateLogger<Contact>(); 

            using var daemon = store.BuildProjectionDaemon(microsoftLogger);
            await daemon.RebuildProjection("Contact", CancellationToken.None);
            System.Console.WriteLine("Done");
        }

        public static Action<StoreOptions, string> ConfigureMarten => (_, connectionString) =>
        {
            _.Connection(connectionString);
            _.Policies.AllDocumentsAreMultiTenanted();
            _.CreateDatabasesForTenants(c =>
            {
                c.ForTenant()
                    .CheckAgainstPgDatabase()
                    .WithEncoding("UTF-8")
                    .ConnectionLimit(-1);

            });

            _.UseDefaultSerialization(serializerType: SerializerType.SystemTextJson);
            _.UseNodaTime(false);
            ((SystemTextJsonSerializer)_.Serializer()).Customize(ConfigureJsonSerializerOptions);

            _.Events.TenancyStyle = TenancyStyle.Conjoined;
            _.Events.StreamIdentity = StreamIdentity.AsGuid;
            _.GeneratedCodeMode = TypeLoadMode.LoadFromPreBuiltAssembly;

            _.Projections.Add(new ContactProjection());
        };

        private static void ConfigureJsonSerializerOptions(JsonSerializerOptions options)
        {
            options.Converters.Add(new JsonStringEnumConverter());
            options.Converters.Add(new TupleConverterFactory());
            options.ConfigureForNodaTime(DateTimeZoneProviders.Tzdb);
            options.SetupExtensions();
            options.GetDictionaryKeyConverterRegistry().RegisterDictionaryKeyConverter(new Utf8DictionaryKeyConverter<Guid>());
            var registry = options.GetDiscriminatorConventionRegistry();
            registry.RegisterConvention(new DefaultDiscriminatorConvention<string>(options));
            registry.DiscriminatorPolicy = DiscriminatorPolicy.Always;
            Action<ObjectMapping<T>> SetDiscriminator<T>() => objectMapping => objectMapping.AutoMap().SetDiscriminator(typeof(T).Name);
            var objectMap = options.GetObjectMappingRegistry();
        }
    }
}