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
    public record Site(string Name);
}

namespace Test
{
    public interface IEvent
    {
        Guid Id { get; }
    }
    public interface ICreateEvent : IEvent { }
    public interface IEntity
    {
        Guid Id { get; }
    }

    public interface ICreateSite {
        Guid Id { get; }
    }
    // public abstract record CreateSite(Guid Id);
    public record SiteCreated(Guid Id, Value.Site Site) : ICreateSite;
    public record SiteCreated2(Guid Id, Value.Site Site) : ICreateSite;
    public record FooCreated(Guid Id);
    public record SiteEdited(Guid Id, Value.Site Site) : IEvent;

    public record Site(Guid Id, Value.Site Value) : IEntity
    {
        public static Site Create(ICreateSite ev) => ev switch
        {
            SiteCreated e => new(ev.Id, e.Site),
            SiteCreated2 e => new(ev.Id, e.Site),
            _ => new(Guid.Empty, new("This should never happen"))
        };

        // public static Site Create(SiteCreated ev) => new(ev.Id, ev.Site);
        // public static Site Create(SiteCreated2 ev) => new(ev.Id, ev.Site);

        public static Site Apply(Site state, IEvent ev) => ev switch
        {
            SiteEdited e => state with { Value = e.Site },
            _ => state
        };
    }

    public class SiteProjection : AggregateProjection<Site>
    {
        public SiteProjection()
        {
            ProjectionName = nameof(Site);
            Lifecycle = ProjectionLifecycle.Inline;

            CreateEvent<ICreateSite>(Site.Create);
            // CreateEvent<ICreateSite>(e =>
            // {
            //     System.Console.WriteLine(e);
            //     return Site.Create(e);
            // });
            // CreateEvent<SiteCreated>(Site.Create);
            // CreateEvent<SiteCreated2>(Site.Create);
            ProjectEvent<SiteEdited>(Site.Apply);
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

            var foldSites = FoldDictionary<Site>(Site.Apply, ev => ev switch
            {
                SiteCreated e => Site.Create(e),
                SiteCreated2 e => Site.Create(e),
                _ => null
            });

            await using var session = store.OpenSession(Guid.NewGuid().ToString());
            var site = Guid.NewGuid();
            var site2 = Guid.NewGuid();
            var createEv = new SiteCreated(site, new("Test"));
            var createEv2 = new SiteCreated2(site2, new("Test"));
            var editEv = new SiteEdited(site, new("Test 2"));
            var createFoo = new FooCreated(Guid.NewGuid());

            session.Events.StartStream(createEv.Id, createEv);
            session.SaveChanges();
            session.Events.Append(editEv.Id, editEv);
            session.SaveChanges();
            session.Events.StartStream(createEv2.Id, createEv2);
            session.SaveChanges();
            session.Events.StartStream(createFoo.Id, createFoo);
            session.SaveChanges();

            var logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .WriteTo.Async(a => a.Console());
            var microsoftLogger = new SerilogLoggerFactory(logger.CreateLogger()).CreateLogger<Site>();

            using var daemon = store.BuildProjectionDaemon(microsoftLogger);
            await daemon.RebuildProjection("Site", CancellationToken.None);

            var sites = await session.Query<Site>().ToListAsync();
            foreach (var s in sites)
                System.Console.WriteLine(s);
            System.Console.WriteLine("Done");
        }

        protected static Func<Dictionary<Guid, T>, IEvent, Dictionary<Guid, T>> FoldDictionary<T>(Func<T, IEvent, T> fold, Func<ICreateEvent, T?> defaultState, Type? deleteEvent = null)
            => new Func<Dictionary<Guid, T>, IEvent, Dictionary<Guid, T>>(
                (dictionary, ev) =>
                {
                    if (ev is IEvent e)
                    {
                        if (ev.GetType() == deleteEvent)
                            dictionary.Remove(ev.Id);
                        else if (dictionary.ContainsKey(ev.Id))
                            dictionary[ev.Id] = fold(dictionary[ev.Id], ev);
                        else
                            if (ev is ICreateEvent create)
                        {
                            var state = defaultState(create);
                            if (state is null) return dictionary;
                            dictionary.Add(ev.Id, fold(state, ev));
                        }

                    }
                    return dictionary;
                });


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