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
using System.Threading.Tasks;
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

	private readonly GQIDateTimeArgument initialTimeArg = new GQIDateTimeArgument("Initial Time")
	{
		IsRequired = false,
	};

	private readonly GQIDateTimeArgument finalTimeArg = new GQIDateTimeArgument("Final Time")
	{
		IsRequired = false,
	};

	private readonly object dictionaryLock = new object();
	private GQIDMS _dms;
	private string frontEndElement = String.Empty;
	private int columnPid = 0;
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

	public void GetServiceGroupsTables(Dictionary<string, FiberNodeOverview> fiberNodeDictionary)
	{
		if (String.IsNullOrEmpty(frontEndElement))
		{
			return;
		}

		var ccapCollecttorTable = GetTable(frontEndElement, 1200000, new List<string>
		{
			"forceFullTable=true;columns=1200002",
		});

		if (ccapCollecttorTable != null && ccapCollecttorTable.Any())
		{
			Parallel.For(0, ccapCollecttorTable[0].Count(), i =>
			{
				var ccapId = Convert.ToString(ccapCollecttorTable[1][i].CellValue);
				var ccapIdArr = ccapId.Split('/');
				var element = new GetElementByIDMessage(Convert.ToInt32(ccapIdArr[0]), Convert.ToInt32(ccapIdArr[1]));
				var paramChange = (ElementInfoEventMessage)_dms.SendMessage(element);
				var protocol = Convert.ToString(paramChange.Protocol);
				if (protocol.Equals("CISCO CBR-8 CCAP Platform") || protocol.Equals("Harmonic CableOs"))
				{
					List<HelperPartialSettings[]> ccapEntityTable = GetTable(ccapId, entityCcapTablePid, new List<string>
					{
							String.Format("forceFullTable=true;columns={0}", entityCcapTablePid+2),
					});
					if (ccapEntityTable != null && ccapEntityTable.Any())
					{
						var paramsToRequest = ccapEntityTable[0].Select(x => new ParameterIndexPair { ID = columnPid, Index = Convert.ToString(x.CellValue) }).ToArray();

						List<ParameterIndexPair[]> parameterPartitions = GetKeysPartition(ccapEntityTable);
						var keysToSelect = ccapEntityTable[0].Select(x => x.CellValue).ToArray();

						CreateRowsDictionary(fiberNodeDictionary, ccapId, ccapEntityTable, parameterPartitions, keysToSelect);
					}
				}
			});
		}

		return;
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

	public string ParseDoubleValue(double doubleValue, string unit)
	{
		if (doubleValue.Equals(-1))
		{
			return "N/A";
		}

		return doubleValue.ToString("0.00") + " " + unit;
	}

	private void CreateRowsDictionary(Dictionary<string, FiberNodeOverview> fibernodeDictionary, string key, List<HelperPartialSettings[]> entityTable, List<ParameterIndexPair[]> parameterPartitions, object[] keysToSelect)
	{
		Parallel.ForEach(parameterPartitions, partition =>
		{
			GetTrendDataResponseMessage trendMessage = GetTrendMessage(key, partition);
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
						fibernodeDictionary[keyFn] = new FiberNodeOverview
						{
							Key = keyFn,
							FiberNodeName = fiberNodeName,
							PeakUtilization = record.Value
								.Select(x => (x as AverageTrendRecord)?.AverageValue ?? -1)
								.DefaultIfEmpty(-1)
								.Max(),
						};
					}
				}
			}
		});
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
}

public class FiberNodeOverview
{
	public string Key { get; set; }

	public string FiberNodeName { get; set; }

	public double PeakUtilization { get; set; }
}

public class HelperPartialSettings
{
	public object CellValue { get; set; }

	public object DisplayValue { get; set; }

	public ParameterDisplayType DisplayType { get; set; }
}