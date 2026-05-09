"""Credential helpers for resolving Unity Catalog service credentials.

The data source supports authenticating to AWS via a UC service credential
(`credential_name` option). Resolving the credential requires different APIs
depending on where the call is happening:

- On a Spark executor (inside a Python UDF / data source `read()` / `write()`,
  with `TaskContext.get()` returning a context),
  `databricks.service_credentials.getServiceCredentialsProvider()` works and
  returns an auto-refreshing botocore `Session`.
- From a notebook itself, `dbutils.credentials.getServiceCredentialsProvider()`
  works and returns the same kind of Session.

Both return an auto-refreshing botocore `Session` that can be passed directly
to `boto3.Session(botocore_session=...)` so boto3 handles credential refresh
natively.

There is no working API for the data source's `schema()` callback when
`credential_name` is set: on both classic DBR and serverless / Spark Connect
the callback runs in a separate Python child process from the notebook, so
`dbutils` is unreachable, and `databricks.service_credentials` rejects calls
outside a UDF context. Users must pass an explicit `.schema(...)` to the
reader to skip schema inference. Schema callbacks for non-credential paths
(static AWS keys) still work because they don't need either API.
"""


def _driver_dbutils():
    """Return the driver-side `dbutils` object on a Databricks runtime.

    `dbutils` is injected into the notebook's IPython user namespace by the
    kernel. This lookup succeeds when the helper is called from the notebook
    process itself; it fails (raising) when called from a Python Data Source
    callback child process, which is the situation in `schema()`/`reader()`/
    `writer()`. Callers in those contexts must use the executor-side API
    (`databricks.service_credentials.getServiceCredentialsProvider`) instead.

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


def get_botocore_session(credential_name):
    """Return an auto-refreshing botocore Session for a UC service credential.

    Picks the right API based on where this code is running:

    - Executor (`TaskContext.get()` returns a context): use
      `databricks.service_credentials.getServiceCredentialsProvider`.
    - Notebook driver: use `dbutils.credentials.getServiceCredentialsProvider`
      via `_driver_dbutils()`.

    Returns None when `credential_name` is empty.
    """
    if not credential_name:
        return None

    from pyspark import TaskContext

    if TaskContext.get() is not None:
        from databricks.service_credentials import getServiceCredentialsProvider
        return getServiceCredentialsProvider(credential_name)

    return _driver_dbutils().credentials.getServiceCredentialsProvider(credential_name)
