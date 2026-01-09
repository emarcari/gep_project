from datetime import date


class PayloadFactory:
    @staticmethod
    def daily_series(
        dataset_id: str,
        report_id: str,
        visual_id: str,
        start_date: date,
        end_date: date,
        product: str,
        department: str,
    ) -> dict:

        start = start_date.strftime("%Y-%m-%dT00:00:00")
        end = end_date.strftime("%Y-%m-%dT00:00:00")

        payload_template = {
            "version": "1.0.0",
            "modelId": 6878420,
            "allowLongRunningQueries": True,
            "userPreferredLocale": "en-US",
            "cancelQueries": [],
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [
                                            {
                                                "Entity": "Precios reuters diarios",
                                                "Name": "p",
                                                "Type": 0,
                                            },
                                            {
                                                "Entity": "Calendario",
                                                "Name": "c",
                                                "Type": 0,
                                            },
                                        ],
                                        "Select": [
                                            {
                                                "Aggregation": {
                                                    "Expression": {
                                                        "Column": {
                                                            "Expression": {
                                                                "SourceRef": {
                                                                    "Source": "p"
                                                                }
                                                            },
                                                            "Property": "PRECIO",
                                                        }
                                                    },
                                                    "Function": 1,
                                                },
                                                "Name": "Sum(Precios reuters diarios.PRECIO)",
                                                "NativeReferenceName": "PRECIO1",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "c"}
                                                    },
                                                    "Property": "Fecha",
                                                },
                                                "Name": "Calendario.Fecha",
                                                "NativeReferenceName": "Fecha",
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "p"}
                                                    },
                                                    "Property": "PRODUCTO",
                                                },
                                                "Name": "Precios reuters diarios.PRODUCTO",
                                                "NativeReferenceName": "PRODUCTO",
                                            },
                                        ],
                                        "Where": [
                                            {
                                                "Condition": {
                                                    "And": {
                                                        "Left": {
                                                            "Comparison": {
                                                                "ComparisonKind": 2,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "c"
                                                                            }
                                                                        },
                                                                        "Property": "Fecha",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": f"datetime'{start}'"
                                                                    }
                                                                },
                                                            }
                                                        },
                                                        "Right": {
                                                            "Comparison": {
                                                                "ComparisonKind": 3,
                                                                "Left": {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "c"
                                                                            }
                                                                        },
                                                                        "Property": "Fecha",
                                                                    }
                                                                },
                                                                "Right": {
                                                                    "Literal": {
                                                                        "Value": f"datetime'{end}'"
                                                                    }
                                                                },
                                                            }
                                                        },
                                                    }
                                                }
                                            },
                                            {
                                                "Condition": {
                                                    "In": {
                                                        "Expressions": [
                                                            {
                                                                "Column": {
                                                                    "Expression": {
                                                                        "SourceRef": {
                                                                            "Source": "p"
                                                                        }
                                                                    },
                                                                    "Property": "DEPARTAMENTO",
                                                                }
                                                            }
                                                        ],
                                                        "Values": [
                                                            [
                                                                {
                                                                    "Literal": {
                                                                        "Value": f"'{department}'"
                                                                    }
                                                                }
                                                            ]
                                                        ],
                                                    }
                                                }
                                            },
                                            {
                                                "Condition": {
                                                    "In": {
                                                        "Expressions": [
                                                            {
                                                                "Column": {
                                                                    "Expression": {
                                                                        "SourceRef": {
                                                                            "Source": "p"
                                                                        }
                                                                    },
                                                                    "Property": "PRODUCTO",
                                                                }
                                                            }
                                                        ],
                                                        "Values": [
                                                            [
                                                                {
                                                                    "Literal": {
                                                                        "Value": f"'{product}'"
                                                                    }
                                                                }
                                                            ]
                                                        ],
                                                    }
                                                }
                                            },
                                        ],
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [{"Projections": [0, 1]}]
                                        },
                                        "Secondary": {
                                            "Groupings": [{"Projections": [2]}]
                                        },
                                        "DataReduction": {
                                            "DataVolume": 4,
                                            "Intersection": {"BinnedLineSample": {}},
                                        },
                                        "Version": 1,
                                    },
                                    "ExecutionMetricsKind": 1,
                                }
                            }
                        ]
                    },
                    "QueryId": "",
                    "ApplicationContext": {
                        "DatasetId": f"'{dataset_id}",
                        "Sources": [
                            {
                                "ReportId": f"'{report_id}",
                                "VisualId": f"'{visual_id}",
                            }
                        ],
                    },
                }
            ],
        }

        return payload_template
