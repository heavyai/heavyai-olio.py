import prefect
from prefect import task, Task, Flow, Parameter, unmapped, apply_map
from prefect.core.task import Task
from prefect.tasks.secrets import EnvVarSecret
from prefect.engine.signals import LOOP

from omnisci_olio.workflow import connect


def _fullclassname(obj):
    return obj.__class__.__module__ + "." + obj.__class__.__name__


class OmnisciStorageTask(Task):
    """
    Abstract Prefect Task to produce a SQL query operation (by a subclass)
    and store the results in an OmniSci DB table.
    """

    def __init__(
        self,
        drop_target=False,
    ):
        super().__init__(
            name=_fullclassname(self),
            # slug=_fullclassname(self),
        )
        self.drop_target = drop_target

    def gen_sql(self, con, **kwargs):
        """
        Subclasses should return an Ibis expression or SQL query text.
        con - DB connection
        """
        pass

    def store(self, con, sources, target, drop=False, **kwargs):
        """
        Calls gen_sql and `con.store`.
        Subclass may override to store differently.
        """

        query = self.gen_sql(con, **sources, **kwargs)
        return con.store(query, load_table=target, sources=sources, drop=drop)

    def run(self, con_url, sources, target, **kwargs):
        with connect(con_url) as con:
            return self.store(
                con, sources=sources, target=target, drop=self.drop_target, **kwargs
            )


class OmnisciStorageLoopTask(OmnisciStorageTask):
    """
    Abstract Prefect Task to produce a SQL query operation (by a subclass)
    and store the results in an OmniSci DB table
    in multiple steps based on some key column present in the source tables
    and not yet present in the target table.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def gen_sql(self, con, loop_key, **kwargs):
        """
        Subclasses should return an Ibis expression or SQL query text.
        con - DB connection
        """
        pass

    def get_loop_keys(self, con, sources, target):
        """
        Return list of keys that are in the sources, but not in the target.
        """
        pass

    def store(self, con, loop_key, sources, target, **kwargs):
        """
        Calls gen_sql and `con.store`.
        Subclass may override to store differently.
        """

        query = self.gen_sql(con, loop_key=loop_key, **sources, **kwargs)
        return con.store(query, load_table=target, update_key=loop_key)

    def run(self, con_url, sources, target, forward_target=None, **kwargs):
        """
        Run the task - will be invoked multiple times, controlled by state held in the task context.

        con_url - OmniSci DB connection URL
        sources - list of source table names
        target - target table name
        forward_target - Optional, table name, to be used when the target table is temporary/intermediate;
            before running get_loop_keys or gen_sql for the target table,
            check get_loop_keys(con, sources, forward_target) - if that is None, end the task.

        Background on prefect task loops: https://docs.prefect.io/core/advanced_tutorials/task-looping.html
        """

        loop_payload = prefect.context.get("task_loop_result", {})
        loop_keys_processed = loop_payload.get("loop_keys_processed", [])
        loop_keys = loop_payload.get("loop_keys", None)

        with connect(con_url) as con:

            # only on the first iteration of the loop
            if len(loop_payload) == 0:

                # only drop on the first iteration of the loop
                if self.drop_target:
                    con.drop_table(target)

                if forward_target and loop_keys is None:
                    forward_loop_keys = self.get_loop_keys(con, sources, forward_target)
                    if len(forward_loop_keys) == 0:
                        # if the forward_target does not need any processing, skip the current task
                        return None

            if loop_keys is None:
                loop_keys = self.get_loop_keys(con, sources, target)

            if len(loop_keys) == 0:
                return target

            loop_key = loop_keys.pop(0)

            self.store(con, loop_key, sources, target, **kwargs)

        loop_keys_processed += loop_key

        result = dict(loop_keys=loop_keys, loop_keys_processed=loop_keys_processed)
        raise LOOP(
            message=str(dict(task=_fullclassname(self), result=result)), result=result
        )
