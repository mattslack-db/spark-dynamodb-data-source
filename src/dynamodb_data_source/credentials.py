"""Credential helpers for resolving Unity Catalog service credentials.

The data source supports authenticating to AWS via a UC service credential
(`credential_name` option). Resolving the credential requires different APIs
depending on where the call is happening:

- On a Spark executor (inside a Python UDF / data source `read()` / `write()`),
  use `databricks.service_credentials.getServiceCredentialsProvider()`. The
  driver-side `dbutils` is not available there.
- On the driver, `databricks.service_credentials.getServiceCredentialsProvider()`
  raises because no `TaskContext` is present, so the data source uses the
  notebook-injected `dbutils.credentials.getServiceCredentialsProvider()`.

Both APIs return an auto-refreshing botocore `Session` that can be passed
directly to `boto3.Session(botocore_session=...)` so boto3 handles credential
refresh natively.
"""


def _driver_dbutils():
    """Return the driver-side `dbutils` object on a Databricks runtime.

    On DBR classic clusters `dbutils` is injected into the notebook's
    Python namespace by the kernel — the data source `writer()`/`reader()`
    callbacks run in that same process, so it's reachable via the IPython
    user namespace or `__main__` globals.

    `pyspark.dbutils.DBUtils(spark)` only works in Databricks Connect (remote)
    sessions and `databricks.sdk.runtime.dbutils` only resolves when it can
    construct an SDK `Config` — neither is reliable from inside a Python data
    source callback, so we look up the injected object directly.
    """
    try:
        import IPython
        ipy = IPython.get_ipython()
        if ipy is not None:
            db = ipy.user_ns.get("dbutils")
            if db is not None:
                return db
    except Exception:
        pass

    import sys
    main = sys.modules.get("__main__")
    if main is not None:
        db = getattr(main, "dbutils", None)
        if db is not None:
            return db

    from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]
    return dbutils
