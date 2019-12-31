// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/collector/metrics_collector.h"

#include <cstdint>

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/collector/local_reporter.h"
#include "kudu/collector/nodes_checker.h"
#include "kudu/collector/reporter_base.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(collector_request_merged_metrics);
DECLARE_string(collector_attributes);
DECLARE_string(collector_cluster_level_metrics);
DECLARE_string(collector_metrics);
DECLARE_string(collector_metrics_types_for_test);

using std::set;
using std::string;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace collector {

scoped_refptr<MetricsCollector> BuildCollector() {
  scoped_refptr<ReporterBase> reporter(new LocalReporter());
  scoped_refptr<NodesChecker> nodes_checker(new NodesChecker(reporter));
  return new MetricsCollector(nodes_checker, reporter);
}

TEST(TestMetricsCollector, TestConvertStateToInt) {
  int64_t result = 1;
  ASSERT_OK(MetricsCollector::ConvertStateToInt("", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("STOPPED", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGSTOPPED", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGBOOTSTRAPPING", &result));
  ASSERT_EQ(result, 0);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNING", &result));
  ASSERT_EQ(result, 1);
  ASSERT_OK(MetricsCollector::ConvertStateToInt("RUNNINGRUNNING", &result));
  ASSERT_EQ(result, 1);
}

TEST(TestMetricsCollector, TestGetHistValue) {
  {
    vector<MetricsCollector::SimpleHistogram> hist_values({{10, 100}});
    ASSERT_EQ(MetricsCollector::GetHistValue(hist_values), 100);
  }
  {
    vector<MetricsCollector::SimpleHistogram> hist_values({{10, 100},
                                                           {20, 200}});
    ASSERT_EQ(MetricsCollector::GetHistValue(hist_values), 167);
  }
}

TEST(TestMetricsCollector, TestMergeToTableLevelMetrics) {
  // Merge empty metrics.
  {
    vector<MetricsCollector::TablesMetrics> hosts_tables_metrics;
    vector<MetricsCollector::TablesHistMetrics> hosts_tables_hist_metrics;
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    ASSERT_OK(MetricsCollector::MergeToTableLevelMetrics(
      hosts_tables_metrics, hosts_tables_hist_metrics,
      &tables_metrics, &tables_hist_metrics));
    ASSERT_TRUE(tables_metrics.empty());
    ASSERT_TRUE(tables_hist_metrics.empty());
  }
  // Merge multi metrics.
  {
    vector<MetricsCollector::TablesMetrics> hosts_tables_metrics{
        {  // host-1
          {
            "table1",
            {
              {"metric1", 1},
              {"metric2", 2}
            }
          },
          {
            "table2",
            {
              {"metric1", 100},
              {"metric3", 200}
            }
          }
        },
        {  // host-2
          {
            "table1",
            {
              {"metric1", 100},
              {"metric2", 200}
            }
          },
          {
            "table2",
            {
              {"metric1", 1},
              {"metric2", 2}
            }
          },
          {
            "table3",
            {
              {"metric1", 1},
              {"metric2", 2}
            }
          }
        }
    };
    vector<MetricsCollector::TablesHistMetrics> hosts_tables_hist_metrics{
        {  // host-1
          {
            "table1",
            {
              {
                "metric3",
                {
                  {10, 100},
                  {20, 200}
                }
              },
              {
                "metric4",
                {
                  {30, 300},
                  {40, 400}
                }
              }
            }
          },
          {
            "table2",
            {
              {
                "metric3",
                {
                  {10, 200},
                  {20, 300}
                }
              },
              {
                "metric4",
                {
                  {40, 300},
                  {50, 400}
                }
              }
            }
          }
        },
        {  // host-2
          {
            "table1",
            {
              {
                "metric3",
                {
                  {10, 100},
                  {20, 200}
                }
              },
              {
                "metric4",
                {
                  {30, 300},
                  {40, 400}
                }
              }
            }
          },
          {
            "table2",
            {
              {
                "metric3",
                {
                  {10, 200},
                  {20, 300}
                }
              },
              {
                "metric4",
                {
                  {40, 300},
                  {50, 400}
                }
              }
            }
          },
          {
            "table3",
            {
              {
                "metric3",
                {
                  {10, 200},
                  {20, 300}
                }
              },
              {
                "metric4",
                {
                  {40, 300},
                  {50, 400}
                }
              }
            }
          }
        }
    };
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    ASSERT_OK(MetricsCollector::MergeToTableLevelMetrics(
        hosts_tables_metrics, hosts_tables_hist_metrics,
        &tables_metrics, &tables_hist_metrics));
    ASSERT_EQ(tables_metrics, MetricsCollector::TablesMetrics({
        {
          "table1",
          {
            {"metric1", 101},
            {"metric2", 202}
          }
        },
        {
          "table2",
          {
            {"metric1", 101},
            {"metric2", 2},
            {"metric3", 200},
          }
        },
        {
          "table3",
          {
            {"metric1", 1},
            {"metric2", 2}
          }
        }
    }));
    ASSERT_EQ(tables_hist_metrics, MetricsCollector::TablesHistMetrics({
        {
          "table1",
          {
            {
              "metric3",
              {
                {10, 100},
                {20, 200},
                {10, 100},
                {20, 200}
              }
            },
            {
              "metric4",
              {
                {30, 300},
                {40, 400},
                {30, 300},
                {40, 400}
              }
            }
          }
        },
        {
          "table2",
          {
            {
              "metric3",
              {
                {10, 200},
                {20, 300},
                {10, 200},
                {20, 300}
              }
            },
            {
              "metric4",
              {
                {40, 300},
                {50, 400},
                {40, 300},
                {50, 400}
              }
            }
          }
        },
        {
          "table3",
          {
            {
              "metric3",
              {
                {10, 200},
                {20, 300}
              }
            },
            {
              "metric4",
              {
                {40, 300},
                {50, 400}
              }
            }
          }
        }
    }));
  }
}

TEST(TestMetricsCollector, TestMergeToClusterLevelMetrics) {
  // Merge empty metrics.
  {
    MetricsCollector::TablesMetrics tables_metrics;
    MetricsCollector::TablesHistMetrics tables_hist_metrics;
    MetricsCollector::Metrics cluster_metrics;
    ASSERT_OK(MetricsCollector::MergeToClusterLevelMetrics(tables_metrics, tables_hist_metrics,
                                                           &cluster_metrics));
    ASSERT_TRUE(cluster_metrics.empty());
  }
  // Merge multi metrics.
  {
    MetricsCollector::TablesMetrics tables_metrics(
        {
          {
            "table1",
            {
              {"metric1", 100}
            }
          },
          {
            "table2",
            {
              {"metric1", 10},
              {"metric2", 20}
            }
          },
          {
            "table3",
            {
              {"metric1", 1},
              {"metric2", 2},
              {"metric3", 3}
            }
          }
        }
    );
    MetricsCollector::TablesHistMetrics tables_hist_metrics;  // TODO(yingchun) not used now.
    MetricsCollector::Metrics cluster_metrics({{"metric2", 0}});
    ASSERT_OK(MetricsCollector::MergeToClusterLevelMetrics(tables_metrics, tables_hist_metrics,
                                                           &cluster_metrics));
    ASSERT_EQ(cluster_metrics, MetricsCollector::Metrics({
        {
          {"metric2", 22}
        }
    }));
  }
}

TEST(TestMetricsCollector, TestParseMetrics) {
  auto collector = BuildCollector();
  collector->metric_types_ = {
      {"server_metric", "COUNTER"},
      {"metric_counter1", "COUNTER"},
      {"metric_counter2", "COUNTER"},
      {"server_metric_histogram", "HISTOGRAM"},
      {"metric_histogram1", "HISTOGRAM"},
      {"metric_histogram2", "HISTOGRAM"}
  };
  string data(
      R"*([                                             )*"
      R"*(  {                                           )*"
      R"*(    "type": "server",                         )*"
      R"*(    "id": "kudu.tabletserver",                )*"
      R"*(    "attributes": {                           )*"
      R"*(      "attrA": "val1",                        )*"
      R"*(      "attrB": "val2"                         )*"
      R"*(    },                                        )*"
      R"*(    "metrics": [                              )*"
      R"*(      {                                       )*"
      R"*(        "name": "server_metric",              )*"
      R"*(        "value": 123                          )*"
      R"*(      },                                      )*"
      R"*(      {                                       )*"
      R"*(        "name": "server_metric_histogram",    )*"
      R"*(        "total_count": 60,                    )*"
      R"*(        "min": 4,                             )*"
      R"*(        "mean": 76.16666666666667,            )*"
      R"*(        "percentile_75": 25,                  )*"
      R"*(        "percentile_95": 66,                  )*"
      R"*(        "percentile_99": 79,                  )*"
      R"*(        "percentile_99_9": 3486,              )*"
      R"*(        "percentile_99_99": 3486,             )*"
      R"*(        "max": 3486,                          )*"
      R"*(        "total_sum": 4570                     )*"
      R"*(      }                                       )*"
      R"*(    ]                                         )*"
      R"*(  },                                          )*"
      R"*(  {                                           )*"
      R"*(    "type": "table",                          )*"
      R"*(    "id": "table1",                           )*"
      R"*(    "attributes": {                           )*"
      R"*(      "attr1": "val2",                        )*"
      R"*(      "attr2": "val3"                         )*"
      R"*(    },                                        )*"
      R"*(    "metrics": [                              )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_counter1",            )*"
      R"*(        "value": 10                           )*"
      R"*(      },                                      )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_counter2",            )*"
      R"*(        "value": 20                           )*"
      R"*(      },                                      )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_histogram1",          )*"
      R"*(        "total_count": 17,                    )*"
      R"*(        "min": 6,                             )*"
      R"*(        "mean": 47.8235,                      )*"
      R"*(        "percentile_75": 62,                  )*"
      R"*(        "percentile_95": 72,                  )*"
      R"*(        "percentile_99": 73,                  )*"
      R"*(        "percentile_99_9": 73,                )*"
      R"*(        "percentile_99_99": 73,               )*"
      R"*(        "max": 73,                            )*"
      R"*(        "total_sum": 813                      )*"
      R"*(      }                                       )*"
      R"*(    ]                                         )*"
      R"*(  },                                          )*"
      R"*(  {                                           )*"
      R"*(    "type": "table",                          )*"
      R"*(    "id": "table2",                           )*"
      R"*(    "attributes": {                           )*"
      R"*(      "attr1": "val3",                        )*"
      R"*(      "attr2": "val2"                         )*"
      R"*(    },                                        )*"
      R"*(    "metrics": [                              )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_counter1",            )*"
      R"*(        "value": 100                          )*"
      R"*(      },                                      )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_histogram1",          )*"
      R"*(        "total_count": 170,                   )*"
      R"*(        "min": 60,                            )*"
      R"*(        "mean": 478.235,                      )*"
      R"*(        "percentile_75": 620,                 )*"
      R"*(        "percentile_95": 720,                 )*"
      R"*(        "percentile_99": 730,                 )*"
      R"*(        "percentile_99_9": 735,               )*"
      R"*(        "percentile_99_99": 735,              )*"
      R"*(        "max": 735,                           )*"
      R"*(        "total_sum": 8130                     )*"
      R"*(      },                                      )*"
      R"*(      {                                       )*"
      R"*(        "name": "metric_histogram2",          )*"
      R"*(        "total_count": 34,                    )*"
      R"*(        "min": 6,                             )*"
      R"*(        "mean": 47.8235,                      )*"
      R"*(        "percentile_75": 62,                  )*"
      R"*(        "percentile_95": 72,                  )*"
      R"*(        "percentile_99": 72,                  )*"
      R"*(        "percentile_99_9": 73,                )*"
      R"*(        "percentile_99_99": 73,               )*"
      R"*(        "max": 73,                            )*"
      R"*(        "total_sum": 813                      )*"
      R"*(      }                                       )*"
      R"*(    ]                                         )*"
      R"*(  }                                           )*"
      R"*(]                                             )*");

  MetricsCollector::TablesMetrics tables_metrics;
  MetricsCollector::TablesHistMetrics tables_hist_metrics;
  MetricsCollector::Metrics host_metrics;
  MetricsCollector::HistMetrics host_hist_metrics;
  ASSERT_OK(collector->ParseMetrics(MetricsCollector::NodeType::kTServer,
                                    data,
                                    &tables_metrics, &host_metrics,
                                    &tables_hist_metrics, &host_hist_metrics));
  ASSERT_EQ(tables_metrics, MetricsCollector::TablesMetrics({
      {
        "table1",
        {
          {"metric_counter1", 10},
          {"metric_counter2", 20},
        }
      },
      {
        "table2",
        {
          {"metric_counter1", 100}
        }
      }
  }));
  ASSERT_EQ(tables_hist_metrics, MetricsCollector::TablesHistMetrics({
      {
        "table1",
        {
          {
            "metric_histogram1_percentile_99",
            {
              {17, 73}
            }
          }
        }
      },
      {
        "table2",
        {
          {
            "metric_histogram1_percentile_99",
            {
              {170, 730}
            }
          },
          {
            "metric_histogram2_percentile_99",
            {
              {34, 72}
            }
          }
        }
      }
  }));
  ASSERT_EQ(host_metrics, MetricsCollector::Metrics({
      {"metric_counter1", 110},
      {"metric_counter2", 20},
      {"server_metric", 123}
  }));
  ASSERT_EQ(host_hist_metrics, MetricsCollector::HistMetrics({
      {
        "metric_histogram1_percentile_99",
        {
          {17, 73},
          {170, 730}
        }
      },
      {
        "metric_histogram2_percentile_99",
        {
          {34, 72}
        }
      },
      {
        "server_metric_histogram_percentile_99",
        {
          {60, 79}
        }
      }
  }));
}

TEST(TestMetricsCollector, TestInitMetrics) {
  FLAGS_collector_metrics_types_for_test =
      R"*([                                                       )*"
      R"*(  {                                                     )*"
      R"*(    "type": "table",                                   )*"
      R"*(    "id": "table1",                                     )*"
      R"*(    "metrics": [                                        )*"
      R"*(      {                                                 )*"
      R"*(        "name": "counter_metric1",                      )*"
      R"*(        "type": "counter"                               )*"
      R"*(      },                                                )*"
      R"*(      {                                                 )*"
      R"*(        "name": "histogram_metric1",                    )*"
      R"*(        "type": "histogram"                             )*"
      R"*(      },                                                )*"
      R"*(      {                                                 )*"
      R"*(        "name": "gauge_metric1",                        )*"
      R"*(        "type": "gauge"                                 )*"
      R"*(      }                                                 )*"
      R"*(    ]                                                   )*"
      R"*(  },                                                    )*"
      R"*(  {                                                     )*"
      R"*(    "type": "server",                                   )*"
      R"*(    "metrics": [                                        )*"
      R"*(      {                                                 )*"
      R"*(        "name": "counter_metric2",                      )*"
      R"*(        "type": "counter"                               )*"
      R"*(      },                                                )*"
      R"*(      {                                                 )*"
      R"*(        "name": "histogram_metric2",                    )*"
      R"*(        "type": "histogram"                             )*"
      R"*(      },                                                )*"
      R"*(      {                                                 )*"
      R"*(        "name": "gauge_metric2",                        )*"
      R"*(        "type": "gauge"                                 )*"
      R"*(      }                                                 )*"
      R"*(    ]                                                   )*"
      R"*(  }                                                     )*"
      R"*(]                                                       )*";
  auto collector = BuildCollector();
  ASSERT_OK(collector->InitMetrics());
  MetricsCollector::MetricTypes expect_metric_types({
      {"counter_metric1", "COUNTER"},
      {"histogram_metric1", "HISTOGRAM"},
      {"gauge_metric1", "GAUGE"},
      {"counter_metric2", "COUNTER"},
      {"histogram_metric2", "HISTOGRAM"},
      {"gauge_metric2", "GAUGE"}
  });
  ASSERT_EQ(collector->metric_types_, expect_metric_types);
}

TEST(TestMetricsCollector, TestInitFilters) {
  FLAGS_collector_attributes = "attr1:val1,val2;attr2:val1";
  auto collector = BuildCollector();
  ASSERT_OK(collector->InitFilters());
  unordered_map<string, set<string>> expect_attributes_filter({
      {
        "attr1",
        {"val1", "val2"}
      },
      {
        "attr2",
        {"val1"}
      }
  });
  ASSERT_EQ(collector->attributes_filter_, expect_attributes_filter);
}

#define CHECK_URL_PARAMETERS(metrics, request_merged, attributes, expect_url)                     \
do {                                                                                              \
  FLAGS_collector_metrics = metrics;                                                              \
  FLAGS_collector_request_merged_metrics = request_merged;                                        \
  FLAGS_collector_attributes = attributes;                                                        \
  auto collector = BuildCollector();                                                              \
  ASSERT_OK(collector->InitFilters());                                                            \
  ASSERT_OK(collector->InitMetricsUrlParameters());                                               \
  ASSERT_EQ(collector->metric_url_parameters_, expect_url);                                       \
} while (false)

TEST(TestMetricsCollector, TestInitMetricsUrlParameters) {
  CHECK_URL_PARAMETERS("", true, "",
      "/metrics?compact=1&merge_rules=tablet|table|table_name");
  CHECK_URL_PARAMETERS("m1,m2,m3", true, "",
      "/metrics?compact=1&metrics=m1,m2,m3&merge_rules=tablet|table|table_name");
  // TODO(yingchun): now FLAGS_collector_request_merged_metrics must be true
  //CHECK_URL_PARAMETERS("", false, "", "",
  //    "/metrics?compact=1");
  CHECK_URL_PARAMETERS("", true, "attr1:a1,a2;attr2:a3",
      "/metrics?compact=1&merge_rules=tablet|table|table_name"
      "&attributes=attr2,a3,attr1,a1,attr1,a2,");
  CHECK_URL_PARAMETERS("", true, "",
      "/metrics?compact=1&merge_rules=tablet|table|table_name");
}

TEST(TestMetricsCollector, TestInitClusterLevelMetrics) {
  FLAGS_collector_cluster_level_metrics = "m1,m2,m3";
  auto collector = BuildCollector();
  ASSERT_OK(collector->InitClusterLevelMetrics());
  MetricsCollector::Metrics cluster_metrics({
      {"m1", 0},
      {"m2", 0},
      {"m3", 0},
  });
  ASSERT_EQ(collector->cluster_metrics_, cluster_metrics);
}
}  // namespace collector
}  // namespace kudu

