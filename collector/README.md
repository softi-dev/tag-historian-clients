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

The image is published for `linux/amd64` and `linux/arm64`, so the same commands run on a PC or a
server, on Docker Desktop for Windows or macOS, and on ARM boxes with a 64-bit OS - a Raspberry Pi,
a Siemens IOT2050, a Revolution Pi.

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

Everything lives in one Docker volume mounted at `/config`: the configuration goes in; the offline
queue and (for OPC UA) the certificate store come out. You do not write the configuration from
scratch - the first start puts a complete, commented example into the volume for you. Every
command below is a single line, so it pastes the same into bash, zsh and PowerShell.

**1. Start it once to get a config file.**

```bash
docker run --name tag-collector -v tag-collector:/config ghcr.io/softi-dev/tag-historian-clients/collector:latest
```

On a new, empty volume this writes `/config/appsettings.json`, prints what to edit, and exits with
code `78` (configuration required). That is the expected result here, not a failure: nothing has
connected anywhere yet, and the stopped container stays behind so the next step can reach the
volume through it.

**2. Edit it.** Copy the file out and edit it with any editor:

```bash
docker cp tag-collector:/config/appsettings.json appsettings.json
```

Keep the source list you use - `MqttSources` or `OpcServers`, or both - fill in its values, and
delete the example entry from the one you do not use. Every value still in `<angle brackets>` is a
placeholder, and the collector will not start while one is left in; everything else is a default,
with a comment saying what it does. Leave `ApiKey` empty: the key goes into the environment in
step 3, so it is never stored on the volume. Then copy the file back and remove the first-start
container:

```bash
docker cp appsettings.json tag-collector:/config/appsettings.json
docker rm tag-collector
```

If you run docker with `sudo`, use it for all of these and edit the copied file with `sudo` too: a
file docker copied out as root belongs to root.

**3. Run it.**

```bash
docker run -d --name tag-collector --restart unless-stopped -v tag-collector:/config -e TagHistorian__ApiKey="<your-api-key>" ghcr.io/softi-dev/tag-historian-clients/collector:latest
```

Follow it with `docker logs -f tag-collector`. Put your key in place of `<your-api-key>`: left as it
is, it stops the collector with exit code `78` and the name of the variable. To change the
configuration later, copy it out and back in the same way and run `docker restart tag-collector`.

Or as `docker-compose.yml`, once steps 1 and 2 have created the volume and put the config in it -
the volume is `external` for exactly that reason, since a volume compose created would be a new,
empty one:

```yaml
services:
  collector:
    image: ghcr.io/softi-dev/tag-historian-clients/collector:latest
    container_name: tag-collector
    restart: unless-stopped
    volumes:
      - tag-collector:/config
    environment:
      TagHistorian__ApiKey: "<your-api-key>"

volumes:
  tag-collector:
    external: true
```

A collector with no key does not crash-loop: it logs a critical error naming
`TagHistorian__ApiKey` and then sits idle, collecting nothing, until it is restarted with one. The
same applies when every source list is empty - there has to be at least one of `MqttSources`,
`OpcServers` or `SparkplugSources` with an entry in it, or there is nothing to do and the log says
so. A key left at `<your-api-key>`, a placeholder left in the file, and a file that no longer parses
after an edit each stop it at startup with exit code `78` and a message naming the setting, or the
line and position where the parser gave up. Under `--restart unless-stopped` Docker keeps starting
it again, so `docker ps` shows `Restarting (78)` until the configuration is fixed.

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

The example the first start writes lists every setting the collector reads, each with its default
and a comment. Trimmed to what matters, an MQTT-only `appsettings.json` comes down to this; the
other two lists take the other two protocols:

```json
{
  "TagHistorian": {
    "ApiUrl": "https://api.taghistorian.com",
    "ApiKey": ""
  },
  "MqttSources": [
    {
      "Name": "broker",
      "Host": "192.168.1.10",
      "Topics": [ "zigbee2mqtt/+" ]
    }
  ],
  "OpcServers": [],
  "SparkplugSources": []
}
```

Sparkplug B ships commented out in the example, because one edge node can announce thousands of
tags: read its guide first, then remove the leading `// ` from the lines between the
`SparkplugSources` brackets and fill it in.

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

That goes as far as having no file at all. A start whose environment configures at least one
source runs without `appsettings.json` and writes no example. It then needs
`TagHistorian__ApiUrl` as well, since the API address otherwise comes from the file. Keep the
volume anyway: the queue lives on it.

```bash
docker run -d --name tag-collector --restart unless-stopped -v tag-collector:/config -e TagHistorian__ApiUrl=https://api.taghistorian.com -e TagHistorian__ApiKey="<your-api-key>" -e MqttSources__0__Host=192.168.1.10 -e MqttSources__0__Topics__0=zigbee2mqtt/+ ghcr.io/softi-dev/tag-historian-clients/collector:latest
```

## What is on the volume

| Path | Direction | What it is |
|---|---|---|
| `/config/appsettings.json` | yours | The configuration. Written as a commented example by the first start when it is missing, and never touched by the collector after that. |
| `/config/queue/` | the collector's | The store-and-forward queue. Bounded: 168 hours / 512 MiB by default (`TagHistorian:StoreAndForward`), oldest evicted first, every eviction logged at `Error` as data loss. |
| `/config/queue/poison/` | the collector's | Records the API permanently rejected, quarantined as recoverable JSON lines rather than discarded. |
| `/config/pki/` | the collector's | OPC UA certificate stores. The client certificate is generated on first start, and its identity is persisted so a container recreate does not regenerate it and break the trust you established on the PLC side. |
| `/config/sparkplug-discovery/` | the collector's | One JSON inventory per Sparkplug edge node or device, rewritten at each birth, naming every metric it announced. |

Keep the volume. It is the config, the buffer and the established trust, all three.

The collector runs as the non-root user `appuser`, uid 999. A named volume needs nothing for that:
Docker creates it writable for that user. A host directory mounted at `/config` instead has to be
writable by uid 999 (`sudo chown 999 <directory>` on Linux; Docker Desktop's bind mounts already
are), or the collector cannot create its queue - and a first start exits with code `73`. Where
SELinux confines containers, a host directory also needs `:z` on its `-v` option.

## Exit codes

| Code | Meaning | What to do |
|---|---|---|
| `78` | Configuration required. The first start wrote the example `appsettings.json`; placeholders from it are still in the file; `TagHistorian__ApiKey` is still the guide's `<your-api-key>`; or `appsettings.json` does not parse. The output names each placeholder, or the line and position of the parse error. | Edit `/config/appsettings.json` and start the collector again. For the key, remove the container and run it again with your own key: a container's environment is fixed when it is created. |
| `73` | There was no configuration, and the example could not be written to `/config`. The output names the cause: a directory the container user may not write, a read-only mount (`:ro`, or `--read-only` with nothing mounted at `/config`), root started without the `DAC_OVERRIDE` capability, or another error such as a full disk. | Do what the output says for that cause. For the commonest, a host directory owned by someone else, use a new named volume as in [Install](#install), without a `--user` option (the volume belongs to the image's own user, uid 999), or a host directory uid 999 can write. |

The collector never overwrites an existing `appsettings.json`, and a configuration with at least
one source and no placeholders starts the way it always has.

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
