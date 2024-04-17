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
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Helper;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Trending;
using System;
using System.Collections.Generic;
using System.Linq;

[GQIMetaData(Name = "Top Channel Utilization")]
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

	private readonly GQIStringArgument entityBeTablePidArg = new GQIStringArgument("BE Entity Table PID")
	{
		IsRequired = true,
	};

	private readonly GQIStringArgument entityCcapTablePidArg = new GQIStringArgument("CCAP Entity Table PID")
	{
		IsRequired = true,
	};

	private readonly GQIDateTimeArgument initialTimeArg = new GQIDateTimeArgument("Initial Time")
	{
		IsRequired = false,
	};

	private readonly GQIDateTimeArgument finalTimeArg = new GQIDateTimeArgument("Final Time")
	{
		IsRequired = false,
	};

	private GQIDMS _dms;
	private string frontEndElement = String.Empty;
	private int columnPid = 0;
	private int entityBeTablePid = 0;
	private int entityCcapTablePid = 0;
	private DateTime initialTime;
	private DateTime finalTime;

	private List<GQIRow> listGqiRows = new List<GQIRow> { };

	public OnInitOutputArgs OnInit(OnInitInputArgs args)
	{
		_dms = args.DMS;
		return new OnInitOutputArgs();
	}

	public GQIColumn[] GetColumns()
	{
		return new GQIColumn[]
		{
			new GQIStringColumn("ID"),
			new GQIStringColumn("Fiber Node"),
			new GQIDoubleColumn("Peak Utilization"),
		};
	}

	public GQIArgument[] GetInputArguments()
	{
		return new GQIArgument[]
		{
			frontEndElementArg,
			columnPidArg,
			entityBeTablePidArg,
			entityCcapTablePidArg,
			initialTimeArg,
			finalTimeArg,
		};
	}

	public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
	{
		listGqiRows.Clear();
		try
		{
			frontEndElement = args.GetArgumentValue(frontEndElementArg);
			columnPid = Convert.ToInt32(args.GetArgumentValue(columnPidArg));
			entityBeTablePid = Convert.ToInt32(args.GetArgumentValue(entityBeTablePidArg));
			entityCcapTablePid = Convert.ToInt32(args.GetArgumentValue(entityCcapTablePidArg));
			initialTime = args.GetArgumentValue(initialTimeArg);
			finalTime = args.GetArgumentValue(finalTimeArg);
			var fibernodeDictionary = new Dictionary<string, FiberNodeOverview>();

			GetServiceGroupsTables(fibernodeDictionary);
			AddRows(fibernodeDictionary);
		}
		catch
		{
			listGqiRows = new List<GQIRow>();
		}

		return new OnArgumentsProcessedOutputArgs();
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

	public void GetServiceGroupsTables(Dictionary<string, FiberNodeOverview> fibernodeDictionary)
	{
		if (String.IsNullOrEmpty(frontEndElement))
		{
			return;
		}

		var backendTable = GetTable(frontEndElement, 1200500, new List<string>
			{
				"forceFullTable=true",
			});

		if (backendTable != null && backendTable.Any())
		{
			for (int i = 0; i < backendTable[0].Count(); i++)
			{
				var key = Convert.ToString(backendTable[0][i].CellValue);
				List<HelperPartialSettings[]> backendEntityTable = GetTable(key, entityBeTablePid, new List<string>
					{
						String.Format("forceFullTable=true;columns={0},{1},{2}",entityBeTablePid+1, entityBeTablePid+2, entityBeTablePid+54),
					});

				if (backendEntityTable == null || !backendEntityTable.Any() || backendEntityTable[0].Length == 0)
					continue;

				var ccapSgNameDict = backendEntityTable[3]
					.Zip(backendEntityTable[2], (ccapId, sgName) => new { CcapId = Convert.ToString(ccapId.CellValue), SgName = Convert.ToString(sgName.CellValue) })
					.Zip(backendEntityTable[0], (data, indexKey) => new { Data = data, Key = Convert.ToString(indexKey.CellValue) })
					.GroupBy(x => x.Data.CcapId, x => new ServiceGroupHelper { Key = Convert.ToString(x.Key), ServiceGroupName = Convert.ToString(x.Data.SgName) })
					.ToDictionary(
						x => x.Key,
						x => x.ToList());

				foreach (var item in ccapSgNameDict)
				{
					var eId = item.Key.Split('/');
					var element = new GetElementByIDMessage(Convert.ToInt32(eId[0]), Convert.ToInt32(eId[1]));
					var paramChange = (ElementInfoEventMessage)_dms.SendMessage(element);
					var protocol = Convert.ToString(paramChange.Protocol);
					if (!protocol.Equals("CISCO CBR-8 CCAP Platform") && !protocol.Equals("Harmonic CableOs"))
					{
						continue;
					}

					List<HelperPartialSettings[]> ccapEntityTable = GetTable(item.Key, entityCcapTablePid, new List<string>
						{
							String.Format("forceFullTable=true;columns={0};trend=avg,{1}", entityCcapTablePid+2, columnPid),
						});
					var paramsToRequest = item.Value.Select(x => new ParameterIndexPair { ID = columnPid, Index = x.Key }).ToArray();
					var keysToSelect = ccapEntityTable[0].Select(x => x.CellValue).ToArray();
					CreateRowsDictionary(fibernodeDictionary, item.Key, ccapEntityTable, paramsToRequest, keysToSelect);
				}
			}
		}

		return;
	}

	private void CreateRowsDictionary(Dictionary<string, FiberNodeOverview> fibernodeDictionary, string key, List<HelperPartialSettings[]> entityTable, ParameterIndexPair[] parameterPartitions, object[] keysToSelect)
	{
		GetTrendDataResponseMessage trendMessage = GetTrendMessage(key, parameterPartitions);
		if (trendMessage == null || trendMessage.Records.IsNullOrEmpty())
		{
			return;
		}

		Dictionary<string, double> trendDictionary = trendMessage.Records.Select(x => new
		{
			Key = x.Key.Substring(x.Key.IndexOf('/') + 1),
			AverageTrendRecords = x.Value.Select(y => y as AverageTrendRecord)
			.Where(z => z.Status == 5)
			.Select(z => z.AverageValue)
			.DefaultIfEmpty(-1).Max(),
		}).ToDictionary(x => x.Key, x => x.AverageTrendRecords);
		var partitionKeys = new HashSet<string>(parameterPartitions.Select(p => p.Index));

		foreach (var keyFn in partitionKeys)
		{
			var index = Array.IndexOf(keysToSelect, keyFn);

			if (index != -1 && trendDictionary.TryGetValue(keyFn, out double peakUtilization))
			{
				fibernodeDictionary.Add(keyFn, new FiberNodeOverview
				{
					Key = keyFn,
					FiberNodeName = Convert.ToString(entityTable[1][index].CellValue),
					PeakUtilization = peakUtilization,
				});
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
		for (int startIndex = 0; startIndex < parameterIndexPairs.Count(); startIndex += batchSize)
		{
			int endIndex = Math.Min(startIndex + batchSize, parameterIndexPairs.Count());
			ParameterIndexPair[] partition = parameterIndexPairs.Skip(startIndex).Take(endIndex - startIndex).ToArray();
			parameterPartitions.Add(partition);
		}

		return parameterPartitions;
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

			var gqiRow1 = new GQIRow(listGqiCells1.ToArray());

			listGqiRows.Add(gqiRow1);
		}
	}

	public GQIPage GetNextPage(GetNextPageInputArgs args)
	{
		return new GQIPage(listGqiRows.ToArray())
		{
			HasNextPage = false,
		};
	}

	private GetTrendDataResponseMessage GetTrendMessage(string element, ParameterIndexPair[] parametersToRequest)
	{
		var elementParts = element.Split('/');
		if (elementParts.Length > 1 && Int32.TryParse(elementParts[0], out int dmaId) && Int32.TryParse(elementParts[1], out int elementId))
		{
			GetTrendDataMessage getTrendMessage = new GetTrendDataMessage
			{
				AverageTrendIntervalType = AverageTrendIntervalType.FiveMin,
				DataMinerID = dmaId,
				ElementID = elementId,
				EndTime = finalTime,
				Parameters = parametersToRequest,
				RetrievalWithPrimaryKey = true,
				ReturnAsObjects = true,
				SkipCache = false,
				StartTime = initialTime,
				TrendingType = TrendingType.Average,
			};
			return _dms.SendMessage(getTrendMessage) as GetTrendDataResponseMessage;
		}

		return null;
	}

	public string ParseDoubleValue(double doubleValue, string unit)
	{
		if (doubleValue.Equals(-1))
		{
			return "N/A";
		}

		return Math.Round(doubleValue, 2) + " " + unit;
	}
}

public class BackEndHelper
{
	public string ElementId { get; set; }
}

public class FiberNodeOverview
{
	public string Key { get; set; }

	public string FiberNodeName { get; set; }

	public double PeakUtilization { get; set; }
}

public class ServiceGroupHelper
{
	public string Key { get; set; }

	public string ServiceGroupName { get; set; }
}

public class HelperPartialSettings
{
	public object CellValue { get; set; }

	public object DisplayValue { get; set; }

	public ParameterDisplayType DisplayType { get; set; }
}