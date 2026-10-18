# Grafana

**`home-assistant.json`** — a Grafana dashboard for Home Assistant entities stored
in Tag Historian: pick entities in an **Entity** dropdown and get one chart per
entity, drawn in the tag's unit and labelled with the friendly name Home
Assistant gave it, with a small panel beside each chart saying whether the line
is made of stored readings or of hourly averages. It is the dashboard people
built on InfluxDB, pointed at Tag Historian instead.

The unit is the tag's own. The Tag Historian custom component sends it with
every reading, and history copied with [`../influxdb-migration/`](../influxdb-migration/)
brings it along. Home Assistant's built-in InfluxDB integration, on its default
settings, sends the friendly name but not the unit in a form Tag Historian
reads, so for an entity only that integration writes, set the unit once on the
tag's page in Tag Historian Explorer; until then its chart has no unit.

It uses Grafana Labs' own **Infinity** data source; there is no plugin of ours
to install. Setting that data source up — the base URL, the allowed host, and an
API key whose **Scope** is **Read** in the `Authorization` header as
`Token <your-api-key>` — is the first half of the
[Grafana guide](https://taghistorian.com/docs/grafana). Then **Dashboards → New →
Import**, upload this file, pick the data source, and put your customer id in
the **Customer ID** box at the top (it is on the Settings page in Tag Historian
Explorer).

Each chart makes two requests a minute — the readings, and the tag's own record
for its name and unit — and the table beside it a third. The dashboard refreshes
once a minute on purpose: the tag record is on the API's smaller per-minute
budget, and at that pace a screen showing every tag a plan allows stays well
inside it with several people watching. That arithmetic is checked on every
build of Tag Historian against this file's twin. Make the refresh faster and do
the sums again with the `X-RateLimit-Limit` header the API sends.

Moving off InfluxDB? See [`../influxdb-migration/`](../influxdb-migration/) for
copying your history across first.
