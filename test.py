from utils.config import *
from utils.analysis_requests import get_analysis_requests

from analysis_queries.event_to_event_query import e2e_scatterplot
if __name__ == "__main__":

    # results = get_analysis_requests()
    # print(results[0])
    df = e2e_scatterplot(24, "Running", "duration", "Sleep",
                         "duration", "00u3sfiunsmoyyiG35d7")
    print(df)
    print(df['correlation_result'].iloc[0])
