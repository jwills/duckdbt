import importlib.util
import os
import tempfile

from buenavista.core import BVType, Extension, Session, QueryResult, SimpleQueryResult


class DbtPythonRunner(Extension):
    """An extension for the BuenaVista server that handles Python model generation in dbt."""

    def type(self) -> str:
        return "dbt_python_job"

    def apply(self, params: dict, handle: Session) -> QueryResult:
        mod_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
        mod_file.write(params["module_definition"].lstrip().encode("utf-8"))
        mod_file.close()
        try:
            spec = importlib.util.spec_from_file_location(
                params["module_name"],
                mod_file.name,
            )
            if not spec:
                raise Exception("Failed to load python model as module")
            module = importlib.util.module_from_spec(spec)
            if spec.loader:
                spec.loader.exec_module(module)
            else:
                raise Exception("Module spec did not include a loader")
            # Do the actual work to run the code here
            cursor = handle.cursor()
            dbt = module.dbtObj(handle.load_df_function)
            df = module.model(dbt, cursor)
            module.materialize(df, cursor)
            return SimpleQueryResult("msg", "Success", BVType.TEXT)
        finally:
            os.unlink(mod_file.name)


class DbtLoadSource(Extension):
    """An extension for the BuenaVista server that handles Python model generation in dbt."""

    def __init__(self, plugin_configs: list):
        pass

    def type(self) -> str:
        return "dbt_load_source"

    def apply(self, params: dict, handle: Session) -> QueryResult:
        pass
