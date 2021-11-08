using System.Linq;
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
using Npgsql;
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

    public record SiteCreated(Guid Id, Value.Site Site) : ICreateEvent;
    public record SiteCreated2(Guid Id, Value.Site Site) : ICreateEvent;
    public record SiteEdited(Guid Id, Value.Site Site) : IEvent;
    public record FooCreated(Guid Id) : ICreateEvent;

    public record Site(Guid Id, Value.Site Value) : IEntity
    {
        public static Site Create(ICreateEvent ev) => ev switch
        {
            SiteCreated e => new(ev.Id, e.Site),
            SiteCreated2 e => new(ev.Id, e.Site),
            _ => new(Guid.Empty, new("This should never happen"))
        };

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

            CreateEvent<ICreateEvent>(Site.Create);
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
            var jsonsettings = new JsonSerializerOptions();
            ConfigureJsonSerializerOptions(jsonsettings);

            var tenant = Guid.NewGuid().ToString();
            await using var session = store.OpenSession(tenant);
            var site = Guid.NewGuid();
            var site2 = Guid.NewGuid();
            var createEv = new SiteCreated(site, new("Test"));
            var createEv2 = new SiteCreated2(site2, new("Test"));
            var createFoo = new FooCreated(Guid.NewGuid());

            session.Events.StartStream(createEv.Id, createEv);
            session.SaveChanges();
            session.Events.StartStream(createEv2.Id, createEv2);
            session.SaveChanges();
            session.Events.StartStream(createFoo.Id, createFoo);
            session.SaveChanges();

            var sites = new Dictionary<Guid, Site>();
            Projections.Add(typeof(Site), sites);
            Folders = new (dynamic, dynamic, dynamic)[] {
                Folder<Site, Dictionary<Guid, Site>>(FoldDictionary<Site>(Site.Apply, Site.Create)),
            };
            Update(new() { createEv, createEv2, createFoo });

            await using var npg = new NpgsqlConnection(conn);
            await npg.OpenAsync();
            var t = await npg.BeginTransactionAsync();

            await using var cmd = new NpgsqlCommand("delete from mt_doc_site;", npg);
            await cmd.ExecuteNonQueryAsync();
            System.Console.WriteLine(sites.Count);
            foreach (var s in sites)
            {
                await using (var insert = new NpgsqlCommand("insert into @table values (@id, @tenant_id, @data, @mt_last_Modified, @mt_version, @mt_dotnet_type);", npg))
                {
                    cmd.Parameters.AddWithValue("table", "mt_doc_site");
                    cmd.Parameters.AddWithValue("id", tenant);
                    cmd.Parameters.AddWithValue("tenant_id", s.Key);
                    cmd.Parameters.AddWithValue("data", JsonSerializer.Serialize(s.Value, jsonsettings));
                    cmd.Parameters.AddWithValue("mt_last_Modified", "NOW()");
                    cmd.Parameters.AddWithValue("mt_version", Guid.NewGuid());
                    cmd.Parameters.AddWithValue("mt_dotnet_type", typeof(Site).FullName!);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            await t.CommitAsync();

            var sites2 = await session.Query<Site>().ToListAsync();
            foreach (var s in sites2)
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

        protected static (dynamic, dynamic, dynamic) Folder<T, TProjection>(Func<TProjection, IEvent, TProjection> fold) => (fold, Projections[typeof(T)], new Action<dynamic>(x => Projections[typeof(T)] = x));
        protected static (dynamic, dynamic, dynamic)[] Folders { get; set; } = Array.Empty<(dynamic, dynamic, dynamic)>();
        protected static Dictionary<Type, dynamic> Projections { get; } = new();

        public static List<IEvent> Events { get; } = new();
        public static void Update(List<IEvent> events)
        {
            Events.AddRange(events);
            foreach (var (folder, projection, set) in Folders)
            {
                dynamic p = projection;
                foreach (var ev in events)
                    p = folder(p, ev);
                set(p);
            }
        }

        public void Update(List<ICreateEvent> events)
        {
            Events.AddRange(events);
            foreach (var (folder, projection, set) in Folders)
            {
                dynamic p = projection;
                foreach (var ev in events)
                    p = folder(p, ev);
                set(p);
            }
        }
    }
}