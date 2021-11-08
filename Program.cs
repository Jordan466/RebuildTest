using System.Text.Json;
using System.Text.Json.Serialization;
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
    public record Site(string Name);
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

    public interface ICreateSite {
        Guid Id { get; }
    }
    public record SiteCreated(Guid Id, Value.Site Site) : ICreateSite;
    public record SiteCreated2(Guid Id, Value.Site Site) : ICreateSite;
    public record FooCreated(Guid Id);

    public record Site(Guid Id, Value.Site Value) : IEntity
    {
        public static Site Create(ICreateSite ev) => ev switch
        {
            SiteCreated e => new(ev.Id, e.Site),
            SiteCreated2 e => new(ev.Id, e.Site),
            _ => new(Guid.Empty, new("This should never happen"))
        };
    }

    public class SiteProjection : AggregateProjection<Site>
    {
        public SiteProjection()
        {
            ProjectionName = nameof(Site);
            Lifecycle = ProjectionLifecycle.Inline;

            CreateEvent<ICreateSite>(Site.Create);
        }
    }

    public class Program
    {
        private static ILogger<Site>? logger =>
            new SerilogLoggerFactory(new LoggerConfiguration()
            .MinimumLevel.Debug()
            .Enrich.FromLogContext()
            .Enrich.WithExceptionDetails()
            .WriteTo.Async(a => a.Console()).CreateLogger())
            .CreateLogger<Site>();

        public static async Task Main(string[] args)
        {
            var database = "test";
            var password = "Password12!";
            var conn = $"User ID=postgres;Password={password};Host=localhost;Port=5432;Database={database};";
            var store = DocumentStore.For(_ => ConfigureMarten(_, conn));

            await using var session = store.OpenSession(Guid.NewGuid().ToString());
            var createSite = new SiteCreated(Guid.NewGuid(), new("Test"));
            var createSite2 = new SiteCreated2(Guid.NewGuid(), new("Test"));
            var createFoo = new FooCreated(Guid.NewGuid());

            session.Events.StartStream(createSite.Id, createSite);
            session.SaveChanges();
            session.Events.StartStream(createSite2.Id, createSite2);
            session.SaveChanges();
            session.Events.StartStream(createFoo.Id, createFoo);
            session.SaveChanges();

            using var daemon = store.BuildProjectionDaemon(logger);
            await daemon.RebuildProjection("Site", CancellationToken.None);

            var sites = await session.Query<Site>().ToListAsync();
            foreach (var s in sites)
                System.Console.WriteLine(s);
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
                    .DropExisting()
                    .WithEncoding("UTF-8")
                    .ConnectionLimit(-1);

            });

            _.UseDefaultSerialization(serializerType: SerializerType.SystemTextJson);
            _.UseNodaTime(false);
            ((SystemTextJsonSerializer)_.Serializer()).Customize(ConfigureJsonSerializerOptions);

            _.Events.TenancyStyle = TenancyStyle.Conjoined;
            _.Events.StreamIdentity = StreamIdentity.AsGuid;
            _.GeneratedCodeMode = TypeLoadMode.LoadFromPreBuiltAssembly;

            _.Projections.Add(new SiteProjection());
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