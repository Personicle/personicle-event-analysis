from utils.config import *
from utils.analysis_requests import get_analysis_requests
from database.write_analysis_to_db import write_analysis_results

from analysis_queries.event_to_event_query import e2e_scatterplot
if __name__ == "__main__":

    # results = get_analysis_requests()
    # print(results[0])
    df = e2e_scatterplot(24, "Running", "duration", "Sleep",
                         "duration", "00u3sfiunsmoyyiG35d7")
    print(df)
    print(df['correlation_result'].iloc[0])
    write_analysis_results("00u3sfiunsmoyyiG35d7",
                           df['correlation_result'].iloc[0], "cae1ec8f-e648-4993-ad80-f63f813a0ddc")
