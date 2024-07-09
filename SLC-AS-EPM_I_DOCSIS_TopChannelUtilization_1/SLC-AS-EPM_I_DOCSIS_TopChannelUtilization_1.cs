/*
****************************************************************************
*  Copyright (c) 2024,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

03/04/2024	1.0.0.1		SSU, Skyline	Initial version
****************************************************************************
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Helper;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Trending;

[GQIMetaData(Name = "Top OFDM Channel Utilization")]
public class MyDataSource : IGQIDataSource, IGQIInputArguments, IGQIOnInit
{
    private readonly GQIStringArgument frontEndElementArg = new GQIStringArgument("FE Element")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument columnPidArg = new GQIStringArgument("Column Requested PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringArgument entityCcapTablePidArg = new GQIStringArgument("CCAP Entity Table PID")
    {
        IsRequired = true,
    };

    private readonly GQIStringDropdownArgument channelInformationArg = new GQIStringDropdownArgument("Channel Information", new[] { "OFDM Channels", "US Channels" })
    {
        IsRequired = true,
    };

    private readonly object dictionaryLock = new object();
    private GQIDMS _dms;
    private string frontEndElement = String.Empty;
    private string channelInformation = String.Empty;
    private int columnPid;
    private int entityCcapTablePid;

    private DateTime initialTime;
    private DateTime finalTime;

    private List<GQIRow> listGqiRows = new List<GQIRow> { };

    private int iterator = 0;
    private List<string> allCollectors = new List<string> { };

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIColumn[] GetColumns()
    {
        if (channelInformation == "US Channels")
        {
            return new GQIColumn[]
            {
                new GQIStringColumn("ID"),
                new GQIStringColumn("Fiber Node"),
                new GQIDoubleColumn("Low Split Utilization"),
                new GQIDoubleColumn("High Split Utilization"),
            };
        }
        else
        {
            return new GQIColumn[]
            {
                new GQIStringColumn("ID"),
                new GQIStringColumn("Fiber Node"),
                new GQIDoubleColumn("Peak Utilization"),
            };
        }
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[]
        {
            frontEndElementArg,
            columnPidArg,
            entityCcapTablePidArg,
            channelInformationArg,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        listGqiRows.Clear();
        try
        {
            finalTime = DateTime.Now;
            initialTime = finalTime - new TimeSpan(24, 0, 0);
            frontEndElement = args.GetArgumentValue(frontEndElementArg);
            columnPid = Convert.ToInt32(args.GetArgumentValue(columnPidArg));
            entityCcapTablePid = Convert.ToInt32(args.GetArgumentValue(entityCcapTablePidArg));
            channelInformation = Convert.ToString(args.GetArgumentValue(channelInformationArg));

            allCollectors = GetAllCollectors();
        }
        catch
        {
            listGqiRows = new List<GQIRow>();
        }

        return new OnArgumentsProcessedOutputArgs();
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        try
        {
            listGqiRows.Clear();

            if (iterator == allCollectors.Count)
            {
                return new GQIPage(listGqiRows.ToArray())
                {
                    HasNextPage = false,
                };
            }
            else
            {
                var fibernodeDictionary = new Dictionary<string, FiberNodeOverview>();

                GetServiceGroupsTables(fibernodeDictionary, allCollectors[iterator]);

                if (channelInformation == "US Channels")
                {
                    AddUsRows(fibernodeDictionary);
                }
                else
                {
                    AddRows(fibernodeDictionary);
                }

                iterator++;

                return new GQIPage(listGqiRows.ToArray())
                {
                    HasNextPage = true,
                };
            }
        }
        catch
        {
            return new GQIPage(listGqiRows.ToArray())
            {
                HasNextPage = false,
            };
        }
    }

    public List<HelperPartialSettings[]> GetTable(string element, int tableId, List<string> filter)
    {
        var columns = new List<HelperPartialSettings[]>();

        var elementIds = element.Split('/');
        if (elementIds.Length > 1 && Int32.TryParse(elementIds[0], out int dmaId) && Int32.TryParse(elementIds[1], out int elemId))
        {
            // Retrieve client connections from the DMS using a GetInfoMessage request
            var getPartialTableMessage = new GetPartialTableMessage(dmaId, elemId, tableId, filter.ToArray());
            var paramChange = (ParameterChangeEventMessage)_dms.SendMessage(getPartialTableMessage);

            if (paramChange != null && paramChange.NewValue != null && paramChange.NewValue.ArrayValue != null)
            {
                columns = paramChange.NewValue.ArrayValue
                    .Where(av => av != null && av.ArrayValue != null)
                    .Select(p => p.ArrayValue.Where(v => v != null)
                    .Select(c => new HelperPartialSettings
                    {
                        CellValue = c.CellValue.InteropValue,
                        DisplayValue = c.CellValue.CellDisplayValue,
                        DisplayType = c.CellDisplayState,
                    }).ToArray()).ToList();
            }
        }

        return columns;
    }

    public List<string> GetAllCollectors()
    {
        var currentCollectors = new List<string>();

        if (String.IsNullOrEmpty(frontEndElement))
        {
            return currentCollectors;
        }

        var ccapCollecttorTable = GetTable(frontEndElement, 1200000, new List<string>
        {
            "forceFullTable=true;columns=1200002",
        });

        if (ccapCollecttorTable != null && ccapCollecttorTable.Any())
        {
            for (int i = 0; i < ccapCollecttorTable[0].Count(); i++)
            {
                var ccapId = Convert.ToString(ccapCollecttorTable[1][i].CellValue);
                currentCollectors.Add(ccapId);
            }
        }

        return currentCollectors;
    }

    public void GetServiceGroupsTables(Dictionary<string, FiberNodeOverview> fiberNodeDictionary, string ccapId)
    {
        string filter = channelInformation == "US Channels"
            ? String.Format("forceFullTable=true;columns={0},{1},{2},{3}", entityCcapTablePid + 2, entityCcapTablePid + 6, entityCcapTablePid + 7, entityCcapTablePid + 10)
            : String.Format("forceFullTable=true;columns={0}", entityCcapTablePid + 2);

        var ccapIdArr = ccapId.Split('/');
        var element = new GetElementByIDMessage(Convert.ToInt32(ccapIdArr[0]), Convert.ToInt32(ccapIdArr[1]));
        var paramChange = (ElementInfoEventMessage)_dms.SendMessage(element);
        var protocol = Convert.ToString(paramChange.Protocol);
        if (channelInformation == "US Channels" || protocol.Equals("CISCO CBR-8 CCAP Platform") || protocol.Equals("Harmonic CableOs"))
        {
            RetrieveTrendData(fiberNodeDictionary, filter, ccapId);
        }
    }

    private void RetrieveTrendData(Dictionary<string, FiberNodeOverview> fiberNodeDictionary, string filter, string ccapId)
    {
        List<HelperPartialSettings[]> ccapEntityTable = GetTable(ccapId, entityCcapTablePid, new List<string>
        {
            filter,
        });

        if (ccapEntityTable != null && ccapEntityTable.Any())
        {
            List<ParameterIndexPair[]> parameterPartitions = GetKeysPartition(ccapEntityTable);
            var keysToSelect = ccapEntityTable[0].Select(x => x.CellValue).ToArray();
            if (channelInformation == "US Channels")
            {
                CreateRowsUsDictionary(fiberNodeDictionary, ccapId, ccapEntityTable, parameterPartitions, keysToSelect);
            }
            else
            {
                CreateRowsDictionary(fiberNodeDictionary, ccapId, ccapEntityTable, parameterPartitions, keysToSelect);
            }
        }
    }

    private List<ParameterIndexPair[]> GetKeysPartition(List<HelperPartialSettings[]> backendEntityTable)
    {
        int batchSize = 25;
        var parameterIndexPairs = backendEntityTable[0].Select(cell => new ParameterIndexPair
        {
            ID = columnPid,
            Index = Convert.ToString(cell.CellValue),
        }).ToList();

        List<ParameterIndexPair[]> parameterPartitions = new List<ParameterIndexPair[]>();
        for (int startIndex = 0; startIndex < parameterIndexPairs.Count; startIndex += batchSize)
        {
            int endIndex = Math.Min(startIndex + batchSize, parameterIndexPairs.Count);
            ParameterIndexPair[] partition = parameterIndexPairs.Skip(startIndex).Take(endIndex - startIndex).ToArray();
            parameterPartitions.Add(partition);
        }

        return parameterPartitions;
    }

    public string ParseDoubleValue(double doubleValue, string unit)
    {
        if (doubleValue.Equals(-1))
        {
            return "N/A";
        }

        return doubleValue.ToString("0.00") + " " + unit;
    }

    private void CreateRowsUsDictionary(Dictionary<string, FiberNodeOverview> fibernodeDictionary, string ccapId, List<HelperPartialSettings[]> entityTable, List<ParameterIndexPair[]> parameterPartitions, object[] keysToSelect)
    {
        var timeRange = new TimeSpan(1, 0, 0);

        for (DateTime i = initialTime; i < finalTime; i += timeRange)
        {
            var tempChannels = GetTrendRangeData(ccapId, entityTable, parameterPartitions, keysToSelect, timeRange, i);

            var groupedChannels = tempChannels.GroupBy(x => x.FiberNodeId);
            foreach (var fiberNodeChannels in groupedChannels)
            {
                var channels = fiberNodeChannels.ToList();
                var lowSplits = channels.Where(x => x.Frequency < 65);
                var highSplits = channels.Where(x => x.Frequency >= 65);

                var lowSplitUtilization = lowSplits.Any() ? lowSplits.Average(x => x.Utilization) : -1;
                var highSplitUtilization = highSplits.Any() ? highSplits.Average(x => x.Utilization) : -1;

                if (!fibernodeDictionary.ContainsKey(fiberNodeChannels.Key))
                {
                    fibernodeDictionary[fiberNodeChannels.Key] = new FiberNodeOverview
                    {
                        Key = fiberNodeChannels.Key,
                        FiberNodeName = channels[0].FiberNodeName,
                        LowSplitUtilization = lowSplitUtilization,
                        HighSplitUtilization = highSplitUtilization,
                    };
                }
                else if (fibernodeDictionary[fiberNodeChannels.Key].LowSplitUtilization < lowSplitUtilization && fibernodeDictionary[fiberNodeChannels.Key].HighSplitUtilization <= highSplitUtilization)
                {
                    fibernodeDictionary[fiberNodeChannels.Key] = new FiberNodeOverview
                    {
                        Key = fiberNodeChannels.Key,
                        FiberNodeName = channels[0].FiberNodeName,
                        LowSplitUtilization = lowSplitUtilization,
                        HighSplitUtilization = highSplitUtilization,
                    };
                }
                else
                {
                    // Do nothing
                }
            }
        }
    }

    private List<ChannelOverview> GetTrendRangeData(string ccapId, List<HelperPartialSettings[]> entityTable, List<ParameterIndexPair[]> parameterPartitions, object[] keysToSelect, TimeSpan timeRange, DateTime i)
    {
        var tempChannels = new List<ChannelOverview>();
        foreach (var partition in parameterPartitions)
        {
            GetTrendDataResponseMessage trendMessage = GetTrendMessage(ccapId, partition, i, i + timeRange);
            if (trendMessage == null || trendMessage.Records.IsNullOrEmpty())
            {
                continue;
            }

            foreach (var record in trendMessage.Records)
            {
                var key = record.Key.Substring(record.Key.IndexOf('/') + 1);
                var index = Array.IndexOf(keysToSelect, key);
                var channelName = Convert.ToString(entityTable[1][index].CellValue);
                var fiberNodeId = Convert.ToString(entityTable[2][index].CellValue);
                var fiberNodeName = Convert.ToString(entityTable[3][index].CellValue);
                var frequency = Convert.ToDouble(entityTable[4][index].CellValue);

                if (index != -1 && !String.IsNullOrEmpty(fiberNodeName))
                {
                    lock (dictionaryLock)
                    {
                        var peak = record.Value
                                .Select(x => (x as AverageTrendRecord)?.AverageValue ?? -1)
                                .DefaultIfEmpty(-1)
                                .Average();

                        tempChannels.Add(new ChannelOverview
                        {
                            Key = key,
                            FiberNodeId = fiberNodeId,
                            FiberNodeName = fiberNodeName,
                            ChannelName = channelName,
                            Frequency = frequency,
                            Utilization = peak >= 0 ? peak : -1,
                        });
                    }
                }
            }
        }

        return tempChannels;
    }

    private void CreateRowsDictionary(Dictionary<string, FiberNodeOverview> fibernodeDictionary, string ccapId, List<HelperPartialSettings[]> entityTable, List<ParameterIndexPair[]> parameterPartitions, object[] keysToSelect)
    {
        foreach (var partition in parameterPartitions)
        {
            GetTrendDataResponseMessage trendMessage = GetTrendMessage(ccapId, partition, initialTime, finalTime);
            if (trendMessage == null || trendMessage.Records.IsNullOrEmpty())
            {
                return;
            }

            foreach (var record in trendMessage.Records)
            {
                var keyFn = record.Key.Substring(record.Key.IndexOf('/') + 1);
                var index = Array.IndexOf(keysToSelect, keyFn);
                var fiberNodeName = Convert.ToString(entityTable[1][index].CellValue);

                if (index != -1 && !String.IsNullOrEmpty(fiberNodeName))
                {
                    lock (dictionaryLock)
                    {
                        var peak = record.Value
                                .Select(x => (x as AverageTrendRecord)?.AverageValue ?? -1)
                                .DefaultIfEmpty(-1)
                                .OrderByDescending(x => x)
                                .Take(3)
                                .Average();

                        fibernodeDictionary[keyFn] = new FiberNodeOverview
                        {
                            Key = keyFn,
                            FiberNodeName = fiberNodeName,
                            PeakUtilization = peak >= 0 ? peak : -1,
                        };
                    }
                }
            }
        }
    }

    private void AddUsRows(Dictionary<string, FiberNodeOverview> rows)
    {
        foreach (var sg in rows)
        {
            List<GQICell> listGqiCells1 = new List<GQICell>
            {
                new GQICell
                {
                    Value = Convert.ToString(sg.Key),
                },
                new GQICell
                {
                    Value = Convert.ToString(sg.Value.FiberNodeName),
                },
                new GQICell
                {
                    Value = Convert.ToDouble(sg.Value.LowSplitUtilization),
                    DisplayValue = ParseDoubleValue(sg.Value.LowSplitUtilization, "%"),
                },
                new GQICell
                {
                    Value = Convert.ToDouble(sg.Value.HighSplitUtilization),
                    DisplayValue = ParseDoubleValue(sg.Value.HighSplitUtilization, "%"),
                },
            };

            var gqiRow = new GQIRow(listGqiCells1.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }

    private void AddRows(Dictionary<string, FiberNodeOverview> rows)
    {
        foreach (var sg in rows)
        {
            List<GQICell> listGqiCells1 = new List<GQICell>
            {
                new GQICell
                {
                    Value = Convert.ToString(sg.Key),
                },
                new GQICell
                {
                    Value = Convert.ToString(sg.Value.FiberNodeName),
                },
                new GQICell
                {
                    Value = Convert.ToDouble(sg.Value.PeakUtilization),
                    DisplayValue = ParseDoubleValue(sg.Value.PeakUtilization, "%"),
                },
            };

            var gqiRow = new GQIRow(listGqiCells1.ToArray());

            listGqiRows.Add(gqiRow);
        }
    }

    private GetTrendDataResponseMessage GetTrendMessage(string element, ParameterIndexPair[] parametersToRequest, DateTime startTime, DateTime endTime)
    {
        var elementParts = element.Split('/');
        if (elementParts.Length > 1 && Int32.TryParse(elementParts[0], out int dmaId) && Int32.TryParse(elementParts[1], out int elementId))
        {
            GetTrendDataMessage getTrendMessage = new GetTrendDataMessage
            {
                AverageTrendIntervalType = AverageTrendIntervalType.FiveMin,
                DataMinerID = dmaId,
                ElementID = elementId,
                EndTime = endTime,
                Parameters = parametersToRequest,
                RetrievalWithPrimaryKey = true,
                ReturnAsObjects = true,
                SkipCache = false,
                StartTime = startTime,
                TrendingType = TrendingType.Average,
            };

            return _dms.SendMessage(getTrendMessage) as GetTrendDataResponseMessage;
        }

        return null;
    }
}

public class FiberNodeOverview
{
    public string Key { get; set; }

    public string FiberNodeName { get; set; }

    public double LowSplitUtilization { get; set; }

    public double HighSplitUtilization { get; set; }

    public double PeakUtilization { get; set; }
}

public class ChannelOverview
{
    public string Key { get; set; }

    public string FiberNodeId { get; set; }

    public string FiberNodeName { get; set; }

    public double Frequency { get; set; }

    public string ChannelName { get; set; }

    public double Utilization { get; set; }
}

public class HelperPartialSettings
{
    public object CellValue { get; set; }

    public object DisplayValue { get; set; }

    public ParameterDisplayType DisplayType { get; set; }
}