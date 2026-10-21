# collector

The Tag Historian collector, as a container image:

```
ghcr.io/softi-dev/tag-historian-clients/collector
```

One image that speaks **OPC UA**, **MQTT**, **Sparkplug B** and **Modbus TCP** - each configured by
its own list, any combination, any one alone a valid install, and all but MQTT still marked beta.
It runs on your network, next to the broker, the PLC or the meter, and connects outward in both
directions: to your equipment on the local network, and to the Tag Historian API over HTTPS. No
port forwarding, no inbound firewall rule, nothing on your network exposed to the internet. The
image opens no listening socket and exposes no ports, and it runs as a non-root user.

The image is published for `linux/amd64` and `linux/arm64`, so the same commands run on a PC or a
server, on Docker Desktop for Windows or macOS, and on ARM boxes with a 64-bit OS - a Raspberry Pi,
a Siemens IOT2050, a Revolution Pi. Where Docker is not allowed, the same collector also runs as a
Windows service, downloaded from [the latest release](https://github.com/softi-dev/tag-historian-clients/releases/latest)
of this repository - [Windows, without Docker](#windows-without-docker) below.

A dropped uplink is not a lost reading: every measurement goes through a crash-durable disk queue
before upload, and replays in order when the connection returns - oldest first, in uploads of up
to 5 000 readings, at up to about 2 400 readings a second. The queue is bounded - 7 days / 512 MiB
by default (about 1.5 to 1.7 million readings, seven to eight hours at a Business plan's full
daily allowance), oldest evicted first, loudly in the log - and lives on the same volume as the
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
delete the example entry from the one you do not use. Sparkplug B and Modbus TCP ship commented out
at the end of the file: for either, delete both example entries and remove the leading `// ` from
the lines of its entry ([Configuration](#configuration) says more). For OPC UA, the collector can
list the server's variables for you instead of you typing NodeIds:
[browse](#opc-ua-browse-and-tag-files). Every value still in `<angle brackets>` is a placeholder,
and the collector will not start while one is left in; everything else is a default, with a comment
saying what it does. Leave `ApiKey` empty: the key goes into the environment in step 3, so it is
never stored on the volume. Then copy the file back and remove the first-start container:

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
`OpcServers`, `SparkplugSources` or `ModbusDevices` with an entry in it, or there is nothing to do
and the log says so. A key left at `<your-api-key>`, a placeholder left in the file, and a file that
no longer parses after an edit each stop it at startup with exit code `78` and a message naming the
setting, or the line and position where the parser gave up; so does an OPC UA
[tag file](#the-tag-file) with a row the collector cannot read. Under `--restart unless-stopped`
Docker keeps starting it again, so `docker ps` shows `Restarting (78)` until the configuration is
fixed.

### Updating the collector

A container keeps the image it was created from, so `docker restart tag-collector` starts the same
collector again. To move to the newest, pull it and create the container again on the same volume.
What is on the volume stays: `appsettings.json`, tag files, certificates and the readings not yet
sent. The `-e` options do not: they belong to the container, so give the new one the same ones. If
you no longer have them, list them before you remove the old one:

```bash
docker inspect tag-collector --format '{{json .Config.Env}}'
```

`PATH`, `APP_UID`, `ASPNETCORE_HTTP_PORTS` and the `DOTNET_` entries come with the image; the rest
are yours. Then:

```bash
docker pull ghcr.io/softi-dev/tag-historian-clients/collector:latest
docker stop tag-collector
docker rm tag-collector
docker run -d --name tag-collector --restart unless-stopped -v tag-collector:/config -e TagHistorian__ApiKey="<your-api-key>" ghcr.io/softi-dev/tag-historian-clients/collector:latest
```

with your key in place of `<your-api-key>`, as before, and your other `-e` options beside it. A
setting left out falls back to the file or its default: a raised `MaxQueueBytes` drops back to
512 MiB, and a collector set up only through the environment finds no source, writes the example
file and stops with exit code `78`. `docker stop` lets the collector close its queue before the
container goes, which `docker rm -f` would not. With compose the environment is in
`docker-compose.yml`, so `docker compose pull` and then `docker compose up -d` do the same. On
Windows, run `install.ps1` from the newer zip - see [Windows, without Docker](#windows-without-docker).

OPC UA tag files, [browse](#opc-ua-browse-and-tag-files) and
[Modbus devices](#modbus-tcp-devices-beta) need collector **1.0.47** or later. An older one ignores
`TagsFile` and `ModbusDevices` without a word and does not know `browse`, so update a collector you
started before then.

## Windows, without Docker

For the SCADA PC or the small plant server where IT does not allow Docker, the collector also
builds as a Windows service: one self-contained `TagHistorianCollector.exe` for 64-bit Windows 10,
11 or Server 2016 and later, with nothing else to install.

**Download** `TagHistorianCollector-<version>-win-x64.zip` and its `.sha256` from
[the latest release](https://github.com/softi-dev/tag-historian-clients/releases/latest) of this
repository - every release carries the current collector beside the Home Assistant integration -
and check the zip before you run anything in it:

```powershell
Get-FileHash .\TagHistorianCollector-*-win-x64.zip -Algorithm SHA256
```

The hash must be the one in the `.sha256` file and in the release notes.

The zip holds the program, `install.ps1`, `uninstall.ps1` and a `README.txt`. In an elevated
PowerShell, in the folder it was extracted to:

```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

`install.ps1`:

- copies the program to `C:\Program Files\Tag Historian\Collector`;
- creates the data folder `C:\ProgramData\Tag Historian\Collector`, the Windows counterpart of the
  `/config` volume - configuration, queue, certificates and log - gives it to Administrators and
  makes it readable by administrators, SYSTEM and the service only, because your API key goes in
  it;
- registers the service `TagHistorianCollector`: automatic start, restarted a minute after a crash,
  running as its own virtual account `NT SERVICE\TagHistorianCollector`, which is granted write
  access to its data folder and nothing else;
- registers the Event Log source `Tag Historian Collector`;
- starts the service once. On a new install that start writes the same commented example
  `appsettings.json` the container writes, and the service stops - on purpose, and it stays stopped
  rather than restarting every minute.

Then, from the same elevated prompt:

```powershell
notepad "C:\ProgramData\Tag Historian\Collector\appsettings.json"
Start-Service TagHistorianCollector
Get-Content -Wait -Tail 20 "C:\ProgramData\Tag Historian\Collector\logs\collector.log"
```

Fill the file in as in step 2 above, with one difference: on Windows **the key goes in the file**,
in `ApiKey`. A service does not see a system environment variable set after Windows started, and
the folder is closed to everyone but administrators and the service. Left empty, the key stops the
service with exit code `78` and says so, rather than leaving it running with nothing to send.

- **Logs.** `logs\collector.log` in the data folder carries the lines `docker logs` shows for the
  container, in files of 5 MiB with the four before the current one kept. Warnings and errors also go
  to Event Viewer > Windows Logs > Application, source `Tag Historian Collector`, and so does the
  reason whenever a configuration problem stops the service.
- **By hand.** Stop the service and start `TagHistorianCollector.exe` from an elevated prompt: it
  uses the same folder, logs to the console as well, and stops on Ctrl+C. One collector runs from a
  folder at a time; a second one exits with code `75`. `--data-dir <folder>`, or the environment
  variable `TAGHISTORIAN_DATA_DIR`, points it at another folder, and `install.ps1 -DataDirectory`
  does the same for the service. Its permissions are replaced, so `install.ps1` refuses a drive
  root, and a folder that already holds other files unless also given `-Force`.
- **Upgrade** by downloading the zip from
  [the latest release](https://github.com/softi-dev/tag-historian-clients/releases/latest) and
  running its `install.ps1` the same way: it stops the service, replaces the program, starts it
  again and keeps the folders the service was installed with. The zip's file name carries the
  collector's version, `TagHistorianCollector-1.0.47-win-x64.zip` for 1.0.47; OPC UA tag files,
  browse and Modbus devices need 1.0.47 or later, and an older program ignores `TagsFile` and
  `ModbusDevices` without a word. **Uninstall** with `uninstall.ps1`, from the program folder or
  the extracted zip; it deletes only what `install.ps1` put there. Both keep the data folder, the
  uninstall unless given `-RemoveData`.
- **Not signed yet.** Started from Explorer, the program gets "Windows protected your PC": choose
  **More info**, then **Run anyway**. `-ExecutionPolicy Bypass` is what lets the unsigned install
  script run, for that one run. On Windows 11 with Smart App Control turned on, unsigned programs are
  blocked outright; use the container there until the collector is signed.

## Getting an API key

A key whose **Scope** is **Write**, created in the Tag Historian dashboard under API keys: press
**Create New Key**, name it, and leave **Scope** on **Write**, where the dialog opens. A Read key
is refused with a `403` on the very first write.

A Write key sends measurements, and creates a tag the first time it writes to a name that does
not exist yet - against your tag quota. It cannot issue or revoke API keys, not even its own; it
cannot delete a tag, which is what deletes that tag's history; it cannot change your plan or
touch your team. The key an account recovery hands you is **Admin**, which is the whole account,
and a box sitting on a broker or plant network should not be holding it.

## Configuration

The example the first start writes lists every setting the collector reads, each with its default
and a comment. Trimmed to what matters, an MQTT-only `appsettings.json` comes down to this; the
other three lists take the other three protocols:

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
  "SparkplugSources": [],
  "ModbusDevices": []
}
```

Sparkplug B ships commented out in the example, because one edge node can announce thousands of
tags: read its guide first, then remove the leading `// ` from the lines between the
`SparkplugSources` brackets and fill it in. Modbus TCP ships commented out the same way, as the
last list in the file, `ModbusDevices` - see [Modbus TCP devices](#modbus-tcp-devices-beta) below.

The full per-protocol guides - topic-to-tag mapping, payload shapes, OPC UA security policies and
the certificate exchange, Sparkplug metric filtering, Modbus register addresses and word order -
are the connector docs:

- **MQTT** - [taghistorian.com/docs/mqtt](https://taghistorian.com/docs/mqtt)
- **OPC UA** (beta) - [taghistorian.com/docs/opc-ua](https://taghistorian.com/docs/opc-ua)
- **Sparkplug B** (beta) - [taghistorian.com/docs/sparkplug-b](https://taghistorian.com/docs/sparkplug-b),
  with the metric-filter advice that section insists you read
- **Modbus TCP** (beta) - [taghistorian.com/docs/modbus](https://taghistorian.com/docs/modbus)

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

### Modbus TCP devices (beta)

`ModbusDevices` polls energy meters, older PLCs, and Modbus RTU devices behind a TCP gateway, from
the same collector and through the same disk queue as the other sources. It needs collector 1.0.47
or later. Beta means it has been tested against a Modbus TCP simulator - every data type and word
order, and devices that stop answering - but not yet against a real meter or PLC. If you run it
against one, tell us how it went in this repository's issues.

```json
"ModbusDevices": [
  {
    "Name": "meter",
    "Host": "192.168.1.30",
    "UnitId": 1,
    "PollIntervalMs": 1000,
    "Tags": [
      { "TagName": "meter.voltage", "RegisterType": "Input", "Address": 0, "DataType": "Float32", "Units": "V" },
      { "TagName": "meter.energy", "RegisterType": "Holding", "Address": 100, "DataType": "UInt32", "Scale": 0.01, "Units": "kWh" }
    ]
  }
]
```

- **It only reads.** Holding and input registers, coils and discrete inputs; it never writes a
  register or a coil.
- **`Address` is 0-based.** Holding register 40001 in a manual is `Holding` 0, 30001 is `Input` 0.
  A `Float32` that reads as nonsense is usually in the other word order: try
  `"WordOrder": "LowWordFirst"`.
- **Only changes are sent.** Every tag is read every `PollIntervalMs`, and a reading goes out when
  the value has moved by more than the row's `Deadband` (in its own units, after `Scale`), or when
  it is readable again after an outage. A device that stops answering gets an outage marker in its
  tags' history, never an invented value.
- **A steady value is not sent again.** A ["no data for" alert](https://taghistorian.com/docs/alerts)
  on a tag that rarely moves can then fire while the device is fine. For such a tag, set the
  device's `HeartbeatSeconds`: an unchanged value is then sent again that often, each time a
  reading against your plan, which is why it is off by default.
- **Tags from a CSV file.** `TagsFile` names one in the collector's folder, its header naming the
  columns as the settings of a `Tags` row are named: `TagName` and `Address` required,
  `RegisterType`, `DataType`, `Scale`, `Units` and the rest optional, with the same defaults. A row
  that cannot be used is skipped with an error in the log naming its line, and the device's other
  rows are collected - the opposite of an [OPC UA tag file](#the-tag-file). `TagPrefix` lets
  identical meters share one file. The file is read at start.
- **No serial port.** Modbus RTU over RS-485 goes through a Modbus TCP gateway, one entry per
  device behind it, each with its own `UnitId`.

The [Modbus guide](https://taghistorian.com/docs/modbus) has the rest: address ranges, data types,
word and byte order, scaling, gateways, and what each outage status means.

### Does your appsettings.json say "BatchSize": 100?

That was the default until the image that raised it to 5000, and the first start writes
`appsettings.json` once and never overwrites it, so an install that started on an older image
keeps 100 through every upgrade. The API acknowledges an upload about every two seconds and the
collector waits for each acknowledgement, so 100 holds it to about 50 readings a second - at a
Business plan's daily average of 58 it falls further behind every hour, and any backlog drains at
that rate.

Pull the current image first, then change the line to `"BatchSize": 5000` or delete it - in that
order, because older images send uploads uncompressed, and 5000 readings uncompressed are about
1.1 MB, more than a slow uplink carries inside the collector's 30-second timeout. Lower it only if
a proxy between the collector and the API limits the request size. While the batch size is what
holds the collector back, the store-and-forward health line is followed by a warning that names
the setting in effect.

Uploads are gzip-compressed (`Content-Encoding: gzip`), about a fifteenth of the JSON; a proxy
between the collector and the API must pass the body through unchanged.

## OPC UA: browse and tag files

There is no wildcard on OPC UA. Every tag is one explicit row mapping one `NodeId` to one tag name,
in the server's `Tags` or in a CSV file its `TagsFile` names, and one row is exactly one tag against
your plan. You do not have to type the rows, though: since 1.0.47 the collector lists them for you.
To start that way, delete the example row from the server's `Tags` in step 2 - the collector then
connects to the server with no tags - and browse once step 3 is running.

### Let the collector list the tags: browse

With the server's `EndpointUrl`, and its security settings if it has any, in `appsettings.json`:

```bash
docker run --rm --pull always -v tag-collector:/config ghcr.io/softi-dev/tag-historian-clients/collector:latest browse
```

`browse` connects with the endpoint, security settings and certificate the collector itself uses,
walks the server's address space and writes every variable it finds to `/config/browse-<Name>.csv`,
named after the server's `Name`: one row per node, with a suggested tag name made from its path, its
data type, and its engineering unit where the server publishes one. It runs in a container of its
own beside the collector, on the same volume, needs no API key, collects nothing and writes nothing
to the server. It leaves out what the collector cannot store - text, dates, structures and arrays -
and the diagnostics of the standard `Server` object. On a large server it stops at 10 levels deep
and 5000 variables, and says so at the top of the file; `--start "ns=3;s=DataBlocksGlobal"` lists
one part of the server instead (in double quotes, or the shell ends the command at the semicolon),
and `--help` shows the other options. `--pull always` is there on purpose: an older image left on
the machine does not know `browse`, and would start as a second collector on the same volume
instead.

Copy the list out, delete every row you do not want stored, and copy it back as `tags.csv`:

```bash
docker cp tag-collector:/config/browse-plant.csv tags.csv
docker cp tags.csv tag-collector:/config/tags.csv
```

A PLC easily has more variables than your plan has tags, and browse lists all of them: keep the ones
you want history of. Then set `"TagsFile": "tags.csv"` on the server in `appsettings.json`, copied
out and back in as in step 2, and run `docker restart tag-collector`. The log says how many tags
came from the file, ahead of the usual `Creating subscription with ... tags`. browse writes over its
own `browse-*.csv` files when it runs again, never over a file a `TagsFile` names, so you can browse
again later and compare.

On Windows, run it from an elevated PowerShell - the data folder is open to administrators and the
service only - while the service runs. `--output` puts the list in your Documents folder, where
Excel can open it; save your edit as **CSV UTF-8**, copy it into the data folder, set `TagsFile` to
`tags.csv` in `appsettings.json`, and restart the service:

```powershell
& "C:\Program Files\Tag Historian\Collector\TagHistorianCollector.exe" browse --output "$HOME\Documents\browse-plant.csv"
Copy-Item "$HOME\Documents\browse-plant.csv" "C:\ProgramData\Tag Historian\Collector\tags.csv"
Restart-Service TagHistorianCollector
```

The program has to be 1.0.47 or later, which the name of the zip it came from says: an older one
answers `browse` as a second collector, which stops at once because the service holds the folder.

browse exits with `0` when the list was written, `69` when the server could not be reached or
browsed (the reason is printed), `73` when the list could not be written - including over a file a
`TagsFile` names - `78` when there is no OPC UA server with an `EndpointUrl` to browse yet or
`appsettings.json` does not parse, and `64` for an option it cannot use.

### The tag file

`TagsFile` is read from the folder `appsettings.json` is in (`/config` in the container). Write it
with browse, export it from your PLC software, or type it: any CSV file with these columns will do.

| Column | Default | Notes |
|---|---|---|
| `NodeId` | - | Required. `ns=2;s=Boiler.Temperature` or `ns=3;i=1204`. |
| `TagName` | - | Required. Starts with a letter; letters, digits and `.` `_` `-` only. |
| `Units` | - | Stored on the tag. `Unit` works too. |
| `Description` | - | Stored on the tag. |
| `SamplingIntervalMs` | `1000` | How often the server samples the node. |
| `CompressionDeadband` | - | A filter at the server, as in `Tags` - [below](#compressiondeadband-is-a-filter-at-the-plc). `0,001` is read as well as `0.001`. |
| `Enabled` | `true` | `false` keeps the row and stops collecting it. |

- Column names in any case, with or without spaces, in any order. Other columns, such as browse's
  `DataType`, are not read, and a name the collector does not know is named in the log at start.
- Commas, semicolons or tabs between values, as Excel saves CSV in any language; in a semicolon
  file a NodeId goes in double quotes, which Excel does itself. Excel in a decimal-comma language
  opens a comma file in a single column: run browse with `--delimiter semicolon` for one it splits.
- UTF-8, which is Excel's **CSV UTF-8**. Blank lines and lines starting with `#` are skipped, unless
  the line holds a NodeId.
- A NodeId in both the file and `Tags` is taken as `Tags` has it, and the log says so.
- The file is read at start: restart the collector after you change it. A NodeId the server does
  not know shows up as a per-item error in the log, as in `Tags`, while the rest keep working.

**A row the collector cannot read stops it at start**, with exit code `78` and every bad row's line
and column, lettered as in Excel: a NodeId that is not one, a tag name the API would refuse, the
same NodeId or tag name on two rows, or a tag name another server in `OpcServers` collects too. So
does a `TagsFile` that is not there. It does not start without them, because a tag you listed would
otherwise stay empty with nothing to tell you why. A Modbus tag file is the one that differs: there
a bad row is skipped with an error in the log and the device's other rows are collected
([Modbus TCP devices](#modbus-tcp-devices-beta)).

### CompressionDeadband is a filter at the PLC

`CompressionDeadband`, on a `Tags` row or in a tag file, is a filter at the OPC UA server and
nothing else. The collector asks the server for a `DataChangeFilter` with `DeadbandType.Absolute`:
a distance in the node's own engineering units, so `0.5` on a tag reading around 50 °C means a move
under half a degree never leaves the server. A change of quality alone still gets through. Size it
as you would any PLC deadband; empty asks for no filter.

What Tag Historian keeps is decided by the **tag's own** deadband, a different kind of number: a
fraction of the last stored value, `0.001` (a move over 0.1%) for every new tag. The collector's
number never sets it, and neither does a Modbus row's `Deadband`, which the collector applies itself
before sending. The OPC UA guide shows
[how to change a tag's deadband](https://taghistorian.com/docs/opc-ua#deadband).

## What is on the volume

| Path | Direction | What it is |
|---|---|---|
| `/config/appsettings.json` | yours | The configuration. Written as a commented example by the first start when it is missing, and never touched by the collector after that. |
| `/config/queue/` | the collector's | The store-and-forward queue. Bounded by `TagHistorian:StoreAndForward`: 168 hours / 512 MiB by default (about 1.5 to 1.7 million readings), oldest evicted first, every eviction logged at `Error` as data loss. |
| `/config/queue/poison/` | the collector's | Records the API permanently rejected, quarantined as recoverable JSON lines rather than discarded. |
| `/config/pki/` | the collector's | OPC UA certificate stores. The client certificate is generated on first start, and its identity is persisted so a container recreate does not regenerate it and break the trust you established on the PLC side. |
| `/config/sparkplug-discovery/` | the collector's | One JSON inventory per Sparkplug edge node or device, rewritten at each birth, naming every metric it announced. |
| `/config/browse-<Name>.csv` | the collector's | What [browse](#let-the-collector-list-the-tags-browse) found on an OPC UA server, written over by the next browse. Copy it out to make a tag file of it. |
| `/config/tags.csv` and other tag files | yours | The OPC UA and Modbus tag lists a `TagsFile` names, read at start. |

Keep the volume. It is the config, the buffer and the established trust, all three.

The collector runs as the non-root user `appuser`, uid 999. A named volume needs nothing for that:
Docker creates it writable for that user. A host directory mounted at `/config` instead has to be
writable by uid 999 (`sudo chown 999 <directory>` on Linux; Docker Desktop's bind mounts already
are), or the collector cannot create its queue - and a first start exits with code `73`. Where
SELinux confines containers, a host directory also needs `:z` on its `-v` option.

## Exit codes

| Code | Meaning | What to do |
|---|---|---|
| `78` | Configuration required. The first start wrote the example `appsettings.json`; placeholders from it are still in the file; `TagHistorian__ApiKey` is still the guide's `<your-api-key>`; on Windows, `ApiKey` is empty; `appsettings.json` does not parse; or an OPC UA `TagsFile` is not there or has a row that cannot be read. The output names each placeholder, the line and position of the parse error, or each bad row's line and column. | Edit `/config/appsettings.json`, or the tag file, and start the collector again. For the key, remove the container and run it again with your own key: a container's environment is fixed when it is created. |
| `73` | There was no configuration, and the example could not be written to `/config`. The output names the cause: a directory the container user may not write, a read-only mount (`:ro`, or `--read-only` with nothing mounted at `/config`), root started without the `DAC_OVERRIDE` capability, or another error such as a full disk. On Windows: the data folder, or the example in it, could not be created, opened or written. | Do what the output says for that cause. For the commonest, a host directory owned by someone else, use a new named volume as in [Install](#install), without a `--user` option (the volume belongs to the image's own user, uid 999), or a host directory uid 999 can write. On Windows, run from an elevated prompt, or run `install.ps1` again for the service. |
| `75` | Windows only: another collector is already running from the same data folder - usually the service, when you start one by hand. | Stop the other one first (`Stop-Service TagHistorianCollector`), or give this one a folder of its own with `--data-dir`. |
| `64` | `--data-dir` was given without a folder after it. | Name the folder. |

The collector never overwrites an existing `appsettings.json`, and a configuration with at least
one source and no placeholders starts the way it always has. `browse` has exit codes of its own,
listed [with it](#let-the-collector-list-the-tags-browse).

## The log is the diagnostic surface

In the container the collector logs to stdout and nowhere else - `docker logs tag-collector` is the
whole story. (The Windows service writes the same lines to a file; see
[Windows](#windows-without-docker).)
There is no HTTP health endpoint and no metrics port, because there is no listening socket at
all. Instead, a health line per concern on a fixed cadence (60 s by default): the
store-and-forward line with queue depth, delivery counters and backoff state, and a line per OPC
UA server, Sparkplug source and Modbus device. The Modbus line starts
`modbus health: device=meter connected=True tagsGood=4 tagsBad=0` and goes on with the device's
poll, reading and outage-marker counters. The store-and-forward line is followed by a warning when the
collector is behind with every upload full at a `BatchSize` under 5000, naming the setting in
effect (`TagHistorian:StoreAndForward:BatchSize` where it is set, otherwise
`TagHistorian:BatchSize`). The lines escalate to `Warning` when something needs attention
(the Sparkplug line while its problem counters are actually growing - one sequence gap last
week is history, not a standing alarm; the Modbus line while its device is not connected or any
of its tags could not be read at the last poll), and everything that costs data - retention
eviction, quarantine, an unreadable segment - logs at `Error` with exact counts. Nothing is
dropped silently.

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
