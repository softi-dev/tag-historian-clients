# collector

The Tag Historian collector, as a container image:

```
ghcr.io/softi-dev/tag-historian-clients/collector
```

One image that speaks **OPC UA**, **MQTT** and **Sparkplug B** - each configured by its own list,
any combination, any one alone a valid install. It runs on your network, next to the broker or the
PLC, and connects outward in both directions: to your equipment on the local network, and to the
Tag Historian API over HTTPS. No port forwarding, no inbound firewall rule, nothing on your
network exposed to the internet. The image opens no listening socket and exposes no ports, and it
runs as a non-root user.

A dropped uplink is not a lost reading: every measurement goes through a crash-durable disk queue
before upload, and replays in order when the connection returns. The queue is bounded - 7 days /
512 MiB by default, oldest evicted first, loudly in the log - and lives on the same volume as the
config, so it survives a container restart or an image upgrade for free.

**The source is not in this repository.** Unlike the Python packages next to this directory, the
collector is part of the Tag Historian product: it is built from the product's private repository
and published under this repository's namespace so that its public home - the image, this
documentation, the issue tracker - is somewhere you can actually see. This directory is its
documentation.

## Install

Everything lives in one mounted volume, `/config`: the configuration goes in; the offline queue
and (for OPC UA) the certificate store come out. The image does not ship an `appsettings.json`
and the process exits at startup without one, so the volume with a config file in it *is* the
install:

```bash
docker run -d --name tag-collector \
  -v mydata:/config \
  -e TagHistorian__ApiKey="<your-api-key>" \
  --restart unless-stopped \
  ghcr.io/softi-dev/tag-historian-clients/collector:latest
```

Or as `docker-compose.yml`:

```yaml
services:
  collector:
    image: ghcr.io/softi-dev/tag-historian-clients/collector:latest
    restart: unless-stopped
    volumes:
      - mydata:/config
    environment:
      TagHistorian__ApiKey: "<your-api-key>"

volumes:
  mydata:
```

Leave `ApiKey` empty in the file and pass it as the environment variable, as above. A collector
with no key does not crash-loop: it logs a critical error naming `TagHistorian__ApiKey` and then
sits idle, collecting nothing, until it is restarted with one. The same applies when every source
list is empty - there has to be at least one of `MqttSources`, `OpcServers` or
`SparkplugSources` with an entry in it, or there is nothing to do and the log says so.

## Getting an API key

A key whose **Scope** is **Write**, created in the Tag Historian dashboard under API keys: press
**Create New Key**, name it, and set **Scope** to **Write**. The dialog opens on **Read**, which
is the right default for the product and the wrong choice here - a Read key is refused with a
`403` on the very first write.

A Write key sends measurements, and creates a tag the first time it writes to a name that does
not exist yet - against your tag quota. It cannot issue or revoke API keys, not even its own; it
cannot delete a tag, which is what deletes that tag's history; it cannot change your plan or
touch your team. The key an account recovery hands you is **Admin**, which is the whole account,
and a box sitting on a broker or plant network should not be holding it.

## Configuration

A minimal `appsettings.json` - this one subscribes to an MQTT broker; the other two lists take
the other two protocols:

```json
{
  "TagHistorian": {
    "ApiUrl": "https://api.taghistorian.com",
    "ApiKey": ""
  },
  "MqttSources": [
    {
      "Name": "home",
      "Host": "192.168.1.10",
      "Topics": [ "zigbee2mqtt/+" ]
    }
  ],
  "OpcServers": [],
  "SparkplugSources": []
}
```

The full per-protocol guides - topic-to-tag mapping, payload shapes, OPC UA security policies and
the certificate exchange, Sparkplug metric filtering - are the connector docs:

- **MQTT** - [taghistorian.com/docs/mqtt](https://taghistorian.com/docs/mqtt)
- **OPC UA** - [taghistorian.com/docs/opc-ua](https://taghistorian.com/docs/opc-ua)
- **Sparkplug B** (beta) - the Connectors page in the
  [dashboard](https://app.taghistorian.com/connectors), which carries the full setup guide and
  the metric-filter advice that section insists you read

Any key in the file can also be set - or overridden - from the environment, using `__` as the
section separator, including list elements by index:

```bash
TagHistorian__ApiKey=...
MqttSources__0__Host=192.168.1.10
TagHistorian__StoreAndForward__MaxQueueBytes=1073741824
```

## What is on the volume

| Path | Direction | What it is |
|---|---|---|
| `/config/appsettings.json` | you write it | The configuration. Required - the process exits without it. |
| `/config/queue/` | the collector's | The store-and-forward queue. Bounded: 168 hours / 512 MiB by default (`TagHistorian:StoreAndForward`), oldest evicted first, every eviction logged at `Error` as data loss. |
| `/config/queue/poison/` | the collector's | Records the API permanently rejected, quarantined as recoverable JSON lines rather than discarded. |
| `/config/pki/` | the collector's | OPC UA certificate stores. The client certificate is generated on first start, and its identity is persisted so a container recreate does not regenerate it and break the trust you established on the PLC side. |
| `/config/sparkplug-discovery/` | the collector's | One JSON inventory per Sparkplug edge node or device, rewritten at each birth, naming every metric it announced. |

Keep the volume. It is the config, the buffer and the established trust, all three.

## The log is the diagnostic surface

The collector logs to stdout and nowhere else - `docker logs tag-collector` is the whole story.
There is no HTTP health endpoint and no metrics port, because there is no listening socket at
all. Instead, a health line per concern on a fixed cadence (60 s by default): the
store-and-forward line with queue depth, delivery counters and backoff state, and a line per OPC
UA server and Sparkplug source. The lines escalate to `Warning` when something needs attention
(the Sparkplug line while its problem counters are actually growing - one sequence gap last
week is history, not a standing alarm), and everything that costs data - retention eviction,
quarantine, an unreadable segment - logs at `Error` with exact counts. Nothing is dropped
silently.

## Image tags

| Tag | Moves |
|---|---|
| `latest` | On every release, and on any manual image build from the main branch. |
| `1.2` | Within a minor - patch releases only. |
| `1.2.3` | Never. Pin this if you change-control your plant floor. |

Releases follow the Tag Historian product: a product release cuts a new collector image, and the
release notes live with the product. There is no separate collector versioning.

## License

The contents of this directory are [Apache-2.0](../LICENSE) like everything else in this
repository. The collector program itself is part of the proprietary Tag Historian product - the
image is free to pull and run against your account, and issues are welcome right here, but its
source is not published.
