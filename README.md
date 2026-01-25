# kivoll infrastructure repo

This repo contains the infrastructure (docker compose) for kivoll.

Deploy local infrastructure with:

```bash
docker-compose -f local/docker-compose.yml up -d
```

As of right now, it is more a place to store the files
so that I dont forget them.

### Services
- [kivoll_api](https://github.com/ignyteX-Labs/kivoll_api): API service for kivoll (golang/chi)
- [kivoll_worker](https://github.com/ignyteX-Labs/kivoll_worker): Background worker for kivoll (python, apscheduler)
- [kivoll_frontend](https://github.com/ignyteX-Labs/kivoll_frontend): not yet deployed
- [kivoll_predict](https://github.com/ignyteX-Labs/kivoll_predict): ML executor not yet deployed
- PostgreSQL: database

### Test suite
This repo contains a test suite for the services and mainly database initialization. <br>
To run the tests, use ``uv sync`` to install python dependencies and then run
```bash
uv run pytest
```
in the root directory.

Notice regarding max_

### Environment variables
###### .env.admin
- ``$POSTGRES_USER`` root postgres username
- ``$POSTGRES_PASSWORD`` root user password
- ``$POSTGRES_DB`` default postgres database

###### .env.worker / predict / api (for kivoll_worker container)
- ``$WORKER/PREDICT/API_MIGRATOR_PASSWORD`` password for ``*_migrator`` user
- ``$WORKER/PREDICT/API_APP_PASSWORD`` password for ``*_app`` user
- ``$SCHEDULER_DB_PASSWORD`` password for ``scheduler`` user (only in ``.env.worker``)
- ``$DB_HOST`` postgres host (``host:port``)
- ``$DB_DRIVER`` db driver name (right now usually ``postgresql``)

TODO: tests for initdb

### Database permission model
##### Users:
- **postgres admin**: set via env variables in .env.admin (``$POSTGRES_USER, $POSTGRES_PASSWORD``)
- # langsameralsveit
- **worker_owner**: Owner of worker related tables (``NOLOGIN``)
- **worker_migrator**: User to run migrations with (``$WORKER_MIGRATOR_PASSWORD``)
- **worker_app**: User to run ``kivoll_worker`` with (``$WORKER_APP_PASSWORD``)
- **api_owner**: Owner of api related tables (``NOLOGIN``)
- **api_migrator**: User to run migrations with (``$API_MIGRATOR_PASSWORD``)
- **api_app**: User to run ``kivoll_api`` with (``$API_APP_PASSWORD``)
- **predict_owner**: Owner of predict related tables (``NOLOGIN``)
- **predict_migrator**: User to run migrations with (``$PREDICT_MIGRATOR_PASSWORD``)
- **predict_app**: User to run ``kivoll_predict`` with (``$PREDICT_APP_PASSWORD``)
- **scheduler**: User to run ``kivoll_worker`` scheduler with (``$SCHEDULER_DB_PASSWORD``)

##### Databases:
###### worker_db
- ``worker_db`` is owned by ``worker_owner``
- ``worker_db`` is accessible by ``worker_migrator`` (``DDL``) and ``worker_app`` (``rw``)
- ``worker_db`` is accessible by ``api_app`` (``ro``)

###### jobs_db
- ``jobs_db`` is owned by ``api_owner``
- ``jobs_db`` is accessible by ``api_migrator`` (``DDL``) and ``api_app`` (``rw``)
- ``jobs_db`` is accessible by ``worker_app`` and ``predict_app`` (``UPDATE, SELECT``)

###### userdata_db (currently not in use)
- ``userdata_db`` is owned by ``api_owner``
- ``userdata_db`` is accessible by ``api_migrator`` (``DDL``) and ``api_app`` (``rw``)

##### predict_db
- ``predict_db`` is owned by ``predict_owner``
- ``predict_db`` is accessible by ``predict_migrator`` (``DDL``) and ``predict_app`` (``rw``)
- ``predict_db`` is accessible by ``api_app`` (``ro``)
