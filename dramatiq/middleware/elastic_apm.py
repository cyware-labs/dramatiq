import warnings

try:
    import elasticapm
    from elasticapm.conf import constants
except ImportError:
    warnings.warn(
        "ElasticAPM is not available.  Run `pip install elastic-apm` "
        "to add apm support.", ImportWarning,
    )
from .middleware import Middleware  # noqa


class ElasticAPM(Middleware):

    def before_process_message(self, broker, message):
        self.traceparent = message.options.get("traceparent")
        if self.traceparent:
            self.traceparent = message.options["traceparent"]
            parent = elasticapm.trace_parent_from_headers(message.options)
            elasticapm.get_client().begin_transaction("dramatiq", trace_parent=parent)
            return
        elasticapm.get_client().begin_transaction("dramatiq")

    def after_process_message(self, broker, message, *, result=None, exception=None, status=None):
        client = elasticapm.get_client()
        if status == "failed":
            outcome = constants.OUTCOME.FAILURE
        elif status == "done":
            outcome = constants.OUTCOME.SUCCESS
        else:
            outcome = constants.OUTCOME.UNKNOWN
        if exception:
            client.capture_exception(
                extra={"exception": exception}, handled=False
            )
        elasticapm.set_transaction_outcome(outcome, override=False)
        client.end_transaction(name=message.actor_name, result=status)

    def before_enqueue(self, broker, message, delay):
        if getattr(self, "traceparent", None):
            traceparent = self.traceparent
        else:
            traceparent = elasticapm.get_trace_parent_header()
        traceparent and message.options.update({"traceparent": traceparent})
