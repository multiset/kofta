-define(
    INCREMENT_COUNTER(Name, Value),
    catch folsom_metrics:notify_existing_metric(Name, {inc, Value}, counter)
).
-define(INCREMENT_COUNTER(Name), ?INCREMENT_COUNTER(Name, 1)).

-define(
    UPDATE_HISTOGRAM(Name, Value),
    catch folsom_metrics:notify_existing_metric(Name, Value, histogram)
).
