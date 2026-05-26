# Datasets estáticos

Esta carpeta contiene los datasets estáticos que se cruzan con el stream en la capa analítica (Etapa 4 — Enriquecimiento).

## `wikis.csv`

Tabla de referencia que asocia cada identificador interno de wiki (`wiki`) con metadatos de negocio:

| Columna | Tipo | Significado |
|---|---|---|
| `wiki` | string | Identificador interno (clave de cruce). Ej. `enwiki`, `commonswiki`. |
| `language` | string | Código ISO 639-1 / 639-3 del idioma principal del proyecto. `mul` para proyectos multilingües. |
| `language_name` | string | Nombre legible del idioma. |
| `country` | string | País asociado (o `Global` para proyectos sin geografía). |
| `project_family` | string | Familia del proyecto Wikimedia (`wikipedia`, `commons`, `wikidata`, `wikisource`, etc.). |
| `community_size_bucket` | string | Bucket cualitativo del tamaño de la comunidad: `xlarge`, `large`, `medium`, `small`. |

### Origen y curación

Los valores se derivan manualmente a partir de la documentación pública de Wikimedia y de las estadísticas de [Wikistats](https://stats.wikimedia.org/). El dataset cubre las wikis con mayor volumen en el stream `recentchange`; si llega un evento de una wiki no listada, el job de Spark genera columnas con `unknown` para que el agregado siga siendo válido.

### Uso

El job `spark/jobs/recent_changes_analytics.py` hace `LEFT JOIN` por `wiki` y produce el agregado enriquecido `changes_by_wiki_hour_enriched` con dimensiones adicionales (`language`, `country`, `project_family`, `community_size_bucket`).
