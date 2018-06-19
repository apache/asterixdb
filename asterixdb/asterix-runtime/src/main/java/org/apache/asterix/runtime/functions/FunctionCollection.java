/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.asterix.common.utils.CodeGenHelper;
import org.apache.asterix.om.functions.IFunctionCollection;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionRegistrant;
import org.apache.asterix.runtime.aggregates.collections.FirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.ListifyAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.collections.LocalFirstElementAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.scalar.ScalarSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableGlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableIntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableLocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.SerializableSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.AvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.CountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.GlobalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.IntermediateSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.LocalSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.MinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlAvgAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlCountAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMaxAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlMinAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SqlSumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.std.SumAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.EmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.aggregates.stream.NonEmptyStreamAggregateDescriptor;
import org.apache.asterix.runtime.evaluators.accessors.CircleCenterAccessor;
import org.apache.asterix.runtime.evaluators.accessors.CircleRadiusAccessor;
import org.apache.asterix.runtime.evaluators.accessors.LineRectanglePolygonAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointXCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.PointYCoordinateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalDayAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalHourAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalEndTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDateAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartDatetimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalIntervalStartTimeAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMillisecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMinuteAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalMonthAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalSecondAccessor;
import org.apache.asterix.runtime.evaluators.accessors.TemporalYearAccessor;
import org.apache.asterix.runtime.evaluators.comparisons.EqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.LessThanOrEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.MissingIfEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NanIfEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NegInfIfEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NotEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.NullIfEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.comparisons.PosInfIfEqualsDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryBase64StringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABinaryHexStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ABooleanConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ACircleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADayTimeDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADoubleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ADurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AFloatConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt16ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt32ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt64ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AInt8ConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromDateTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AIntervalStartFromTimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ALineConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APoint3DConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APointConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.APolygonConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ARectangleConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ATimeConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AUUIDFromStringConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AYearMonthDurationConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.ClosedRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OpenRecordConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.OrderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.UnorderedListConstructorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.AndDescriptor;
import org.apache.asterix.runtime.evaluators.functions.AnyCollectionMemberDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ArrayAppendDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CastTypeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CastTypeLaxDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CheckUnknownDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CodePointToStringDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateCircleDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateLineDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateMBRDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreatePointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreatePolygonDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateQueryUIDDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateRectangleDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CreateUUIDDescriptor;
import org.apache.asterix.runtime.evaluators.functions.DeepEqualityDescriptor;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsWithoutOptionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.GetItemDescriptor;
import org.apache.asterix.runtime.evaluators.functions.GetJobParameterByNameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfInfDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfMissingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfMissingOrNullDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfNanDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfNanOrInfDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IfNullDescriptor;
import org.apache.asterix.runtime.evaluators.functions.InjectFailureDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsArrayDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsAtomicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsBooleanDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsMissingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsNullDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsNumberDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsObjectDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsStringDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsSystemNullDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsUnknownDescriptor;
import org.apache.asterix.runtime.evaluators.functions.LenDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NotDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericACosDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericASinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericATan2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericATanDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericAbsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericAddDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericCeilingDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericCosDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericDegreesDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericDivDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericDivideDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericExpDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericFloorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericLnDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericLogDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericModuloDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericMultiplyDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericPowerDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericRadiansDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericRoundDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericRoundHalfToEven2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericRoundHalfToEvenDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericSignDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericSinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericSqrtDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericSubDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericTanDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericTruncDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NumericUnaryMinusDescriptor;
import org.apache.asterix.runtime.evaluators.functions.OrDescriptor;
import org.apache.asterix.runtime.evaluators.functions.RandomDescriptor;
import org.apache.asterix.runtime.evaluators.functions.RandomWithSeedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SleepDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SpatialAreaDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SpatialCellDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SpatialDistanceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringConcatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringEndsWithDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringEqualDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringInitCapDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringJoinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringLTrim2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.StringLTrimDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringLikeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringLowerCaseDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringPositionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRTrim2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRTrimDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpContainsWithFlagDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpLikeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpLikeWithFlagDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpPositionDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpPositionWithFlagDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpReplaceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRegExpReplaceWithFlagsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringRepeatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringReplaceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringReplaceWithLimitDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringReverseDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringSplitDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringStartsWithDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringToCodePointDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringTrim2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.StringTrimDescriptor;
import org.apache.asterix.runtime.evaluators.functions.StringUpperCaseDescriptor;
import org.apache.asterix.runtime.evaluators.functions.Substring2Descriptor;
import org.apache.asterix.runtime.evaluators.functions.SubstringAfterDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SubstringBeforeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SubstringDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SwitchCaseDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToArrayDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToAtomicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToBigIntDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToBooleanDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToDoubleDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToNumberDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToObjectDescriptor;
import org.apache.asterix.runtime.evaluators.functions.ToStringDescriptor;
import org.apache.asterix.runtime.evaluators.functions.UUIDDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryConcatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.BinaryLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.FindBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.PrintBinaryDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromDescriptor;
import org.apache.asterix.runtime.evaluators.functions.binary.SubBinaryFromToDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByIndexDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessByNameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.FieldAccessNestedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldValueDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.GetRecordFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordAddDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordAddFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordConcatDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordConcatStrictDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordLengthDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordMergeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordNamesDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordPairsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordPutDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRemoveDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRemoveFieldsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordRenameDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordUnwrapDescriptor;
import org.apache.asterix.runtime.evaluators.functions.records.RecordReplaceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustDateTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.AdjustTimeForTimeZoneDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDuartionFromDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CalendarDurationFromDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.CurrentTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DateFromUnixTimeInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromDateAndTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DatetimeFromUnixTimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayOfWeekDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DayTimeDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationEqualDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMillisecondsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.DurationFromMonthsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetOverlappingIntervalDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.GetYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalAfterDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBeforeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalBinDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoveredByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalCoversDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalEndsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMeetsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalMetByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlappedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalOverlapsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartedByDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.IntervalStartsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MillisecondsFromDayTimeDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.MonthsFromYearMonthDurationDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.OverlapBinsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.OverlapDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.ParseTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintDateTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.PrintTimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromDatetimeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.TimeFromUnixTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDateInDaysDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromDatetimeInSecsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.UnixTimeFromTimeInMsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationGreaterThanComparatorDescriptor;
import org.apache.asterix.runtime.evaluators.functions.temporal.YearMonthDurationLessThanComparatorDescriptor;
import org.apache.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.RangeDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.SubsetCollectionDescriptor;

/**
 * This class holds a list of function descriptor factories.
 */
public final class FunctionCollection implements IFunctionCollection {

    private static final String FACTORY = "FACTORY";

    private final ArrayList<IFunctionDescriptorFactory> descriptorFactories = new ArrayList<>();

    @Override
    public void add(IFunctionDescriptorFactory descriptorFactory) {
        descriptorFactories.add(descriptorFactory);
    }

    @Override
    public void addGenerated(IFunctionDescriptorFactory descriptorFactory) {
        add(getGeneratedFunctionDescriptorFactory(descriptorFactory.createFunctionDescriptor().getClass()));
    }

    public static FunctionCollection createDefaultFunctionCollection() {
        FunctionCollection fc = new FunctionCollection();

        // array functions
        fc.addGenerated(ArrayAppendDescriptor.FACTORY);

        // unnesting functions
        fc.add(TidRunningAggregateDescriptor.FACTORY);
        fc.add(ScanCollectionDescriptor.FACTORY);
        fc.add(RangeDescriptor.FACTORY);
        fc.add(SubsetCollectionDescriptor.FACTORY);

        // aggregate functions
        fc.add(ListifyAggregateDescriptor.FACTORY);
        fc.add(CountAggregateDescriptor.FACTORY);
        fc.add(AvgAggregateDescriptor.FACTORY);
        fc.add(LocalAvgAggregateDescriptor.FACTORY);
        fc.add(IntermediateAvgAggregateDescriptor.FACTORY);
        fc.add(GlobalAvgAggregateDescriptor.FACTORY);
        fc.add(SumAggregateDescriptor.FACTORY);
        fc.add(LocalSumAggregateDescriptor.FACTORY);
        fc.add(MaxAggregateDescriptor.FACTORY);
        fc.add(LocalMaxAggregateDescriptor.FACTORY);
        fc.add(MinAggregateDescriptor.FACTORY);
        fc.add(LocalMinAggregateDescriptor.FACTORY);
        fc.add(FirstElementAggregateDescriptor.FACTORY);
        fc.add(LocalFirstElementAggregateDescriptor.FACTORY);

        // serializable aggregates
        fc.add(SerializableCountAggregateDescriptor.FACTORY);
        fc.add(SerializableAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableLocalAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableIntermediateAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableGlobalAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableSumAggregateDescriptor.FACTORY);
        fc.add(SerializableLocalSumAggregateDescriptor.FACTORY);

        // scalar aggregates
        fc.add(ScalarCountAggregateDescriptor.FACTORY);
        fc.add(ScalarAvgAggregateDescriptor.FACTORY);
        fc.add(ScalarSumAggregateDescriptor.FACTORY);
        fc.add(ScalarMaxAggregateDescriptor.FACTORY);
        fc.add(ScalarMinAggregateDescriptor.FACTORY);
        fc.add(EmptyStreamAggregateDescriptor.FACTORY);
        fc.add(NonEmptyStreamAggregateDescriptor.FACTORY);

        // SQL aggregates
        fc.add(SqlCountAggregateDescriptor.FACTORY);
        fc.add(SqlAvgAggregateDescriptor.FACTORY);
        fc.add(LocalSqlAvgAggregateDescriptor.FACTORY);
        fc.add(IntermediateSqlAvgAggregateDescriptor.FACTORY);
        fc.add(GlobalSqlAvgAggregateDescriptor.FACTORY);
        fc.add(SqlSumAggregateDescriptor.FACTORY);
        fc.add(LocalSqlSumAggregateDescriptor.FACTORY);
        fc.add(SqlMaxAggregateDescriptor.FACTORY);
        fc.add(LocalSqlMaxAggregateDescriptor.FACTORY);
        fc.add(SqlMinAggregateDescriptor.FACTORY);
        fc.add(LocalSqlMinAggregateDescriptor.FACTORY);

        // SQL serializable aggregates
        fc.add(SerializableSqlCountAggregateDescriptor.FACTORY);
        fc.add(SerializableSqlAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableLocalSqlAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableIntermediateSqlAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableGlobalSqlAvgAggregateDescriptor.FACTORY);
        fc.add(SerializableSqlSumAggregateDescriptor.FACTORY);
        fc.add(SerializableLocalSqlSumAggregateDescriptor.FACTORY);

        // SQL scalar aggregates
        fc.add(ScalarSqlCountAggregateDescriptor.FACTORY);
        fc.add(ScalarSqlAvgAggregateDescriptor.FACTORY);
        fc.add(ScalarSqlSumAggregateDescriptor.FACTORY);
        fc.add(ScalarSqlMaxAggregateDescriptor.FACTORY);
        fc.add(ScalarSqlMinAggregateDescriptor.FACTORY);

        // boolean functions
        fc.add(AndDescriptor.FACTORY);
        fc.add(OrDescriptor.FACTORY);

        // Record constructors / functions
        fc.add(ClosedRecordConstructorDescriptor.FACTORY);
        fc.add(OpenRecordConstructorDescriptor.FACTORY);
        fc.add(RecordConcatDescriptor.FACTORY);
        fc.add(RecordConcatStrictDescriptor.FACTORY);

        // List constructors
        fc.add(OrderedListConstructorDescriptor.FACTORY);
        fc.add(UnorderedListConstructorDescriptor.FACTORY);

        // Sleep function
        fc.add(SleepDescriptor.FACTORY);

        // Inject failure function
        fc.add(InjectFailureDescriptor.FACTORY);

        // Get Job Parameter function
        fc.add(GetJobParameterByNameDescriptor.FACTORY);

        // Switch case
        fc.add(SwitchCaseDescriptor.FACTORY);

        // null functions
        fc.add(IsMissingDescriptor.FACTORY);
        fc.add(IsNullDescriptor.FACTORY);
        fc.add(IsUnknownDescriptor.FACTORY);
        fc.add(IsSystemNullDescriptor.FACTORY);
        fc.add(CheckUnknownDescriptor.FACTORY);
        fc.add(IfMissingDescriptor.FACTORY);
        fc.add(IfNullDescriptor.FACTORY);
        fc.add(IfMissingOrNullDescriptor.FACTORY);

        // uuid generators (zero independent functions)
        fc.add(CreateUUIDDescriptor.FACTORY);
        fc.add(UUIDDescriptor.FACTORY);
        fc.add(CreateQueryUIDDescriptor.FACTORY);
        fc.add(RandomDescriptor.FACTORY);
        fc.add(CurrentDateDescriptor.FACTORY);
        fc.add(CurrentTimeDescriptor.FACTORY);
        fc.add(CurrentDateTimeDescriptor.FACTORY);

        // functions that need generated class for null-handling.

        // Element accessors.
        fc.addGenerated(FieldAccessByIndexDescriptor.FACTORY);
        fc.addGenerated(FieldAccessByNameDescriptor.FACTORY);
        fc.addGenerated(FieldAccessNestedDescriptor.FACTORY);

        fc.addGenerated(AnyCollectionMemberDescriptor.FACTORY);
        fc.addGenerated(GetItemDescriptor.FACTORY);

        // Numeric functions
        fc.add(IfInfDescriptor.FACTORY);
        fc.add(IfNanDescriptor.FACTORY);
        fc.add(IfNanOrInfDescriptor.FACTORY);
        fc.addGenerated(NumericUnaryMinusDescriptor.FACTORY);
        fc.addGenerated(NumericAddDescriptor.FACTORY);
        fc.addGenerated(NumericDivideDescriptor.FACTORY);
        fc.addGenerated(NumericDivDescriptor.FACTORY);
        fc.addGenerated(NumericMultiplyDescriptor.FACTORY);
        fc.addGenerated(NumericSubDescriptor.FACTORY);
        fc.addGenerated(NumericModuloDescriptor.FACTORY);
        fc.addGenerated(NumericPowerDescriptor.FACTORY);
        fc.addGenerated(NotDescriptor.FACTORY);
        fc.addGenerated(LenDescriptor.FACTORY);
        fc.addGenerated(NumericAbsDescriptor.FACTORY);
        fc.addGenerated(NumericCeilingDescriptor.FACTORY);
        fc.addGenerated(NumericFloorDescriptor.FACTORY);
        fc.addGenerated(NumericRoundDescriptor.FACTORY);
        fc.addGenerated(NumericRoundHalfToEvenDescriptor.FACTORY);
        fc.addGenerated(NumericRoundHalfToEven2Descriptor.FACTORY);
        fc.addGenerated(NumericACosDescriptor.FACTORY);
        fc.addGenerated(NumericASinDescriptor.FACTORY);
        fc.addGenerated(NumericATanDescriptor.FACTORY);
        fc.addGenerated(NumericDegreesDescriptor.FACTORY);
        fc.addGenerated(NumericRadiansDescriptor.FACTORY);
        fc.addGenerated(NumericCosDescriptor.FACTORY);
        fc.addGenerated(NumericSinDescriptor.FACTORY);
        fc.addGenerated(NumericTanDescriptor.FACTORY);
        fc.addGenerated(NumericExpDescriptor.FACTORY);
        fc.addGenerated(NumericLnDescriptor.FACTORY);
        fc.addGenerated(NumericLogDescriptor.FACTORY);
        fc.addGenerated(NumericSqrtDescriptor.FACTORY);
        fc.addGenerated(NumericSignDescriptor.FACTORY);
        fc.addGenerated(NumericTruncDescriptor.FACTORY);
        fc.addGenerated(NumericATan2Descriptor.FACTORY);

        // Comparisons.
        fc.addGenerated(EqualsDescriptor.FACTORY);
        fc.addGenerated(GreaterThanDescriptor.FACTORY);
        fc.addGenerated(GreaterThanOrEqualsDescriptor.FACTORY);
        fc.addGenerated(LessThanDescriptor.FACTORY);
        fc.addGenerated(LessThanOrEqualsDescriptor.FACTORY);
        fc.addGenerated(NotEqualsDescriptor.FACTORY);

        // If-Equals functions
        fc.addGenerated(MissingIfEqualsDescriptor.FACTORY);
        fc.addGenerated(NullIfEqualsDescriptor.FACTORY);
        fc.addGenerated(NanIfEqualsDescriptor.FACTORY);
        fc.addGenerated(PosInfIfEqualsDescriptor.FACTORY);
        fc.addGenerated(NegInfIfEqualsDescriptor.FACTORY);

        // Binary functions
        fc.addGenerated(BinaryLengthDescriptor.FACTORY);
        fc.addGenerated(ParseBinaryDescriptor.FACTORY);
        fc.addGenerated(PrintBinaryDescriptor.FACTORY);
        fc.addGenerated(BinaryConcatDescriptor.FACTORY);
        fc.addGenerated(SubBinaryFromDescriptor.FACTORY);
        fc.addGenerated(SubBinaryFromToDescriptor.FACTORY);
        fc.addGenerated(FindBinaryDescriptor.FACTORY);
        fc.addGenerated(FindBinaryFromDescriptor.FACTORY);

        // String functions
        fc.addGenerated(StringLikeDescriptor.FACTORY);
        fc.addGenerated(StringContainsDescriptor.FACTORY);
        fc.addGenerated(StringEndsWithDescriptor.FACTORY);
        fc.addGenerated(StringStartsWithDescriptor.FACTORY);
        fc.addGenerated(SubstringDescriptor.FACTORY);
        fc.addGenerated(StringEqualDescriptor.FACTORY);
        fc.addGenerated(StringLowerCaseDescriptor.FACTORY);
        fc.addGenerated(StringUpperCaseDescriptor.FACTORY);
        fc.addGenerated(StringLengthDescriptor.FACTORY);
        fc.addGenerated(Substring2Descriptor.FACTORY);
        fc.addGenerated(SubstringBeforeDescriptor.FACTORY);
        fc.addGenerated(SubstringAfterDescriptor.FACTORY);
        fc.addGenerated(StringToCodePointDescriptor.FACTORY);
        fc.addGenerated(CodePointToStringDescriptor.FACTORY);
        fc.addGenerated(StringConcatDescriptor.FACTORY);
        fc.addGenerated(StringJoinDescriptor.FACTORY);
        fc.addGenerated(StringRegExpContainsDescriptor.FACTORY);
        fc.addGenerated(StringRegExpContainsWithFlagDescriptor.FACTORY);
        fc.addGenerated(StringRegExpLikeDescriptor.FACTORY);
        fc.addGenerated(StringRegExpLikeWithFlagDescriptor.FACTORY);
        fc.addGenerated(StringRegExpPositionDescriptor.FACTORY);
        fc.addGenerated(StringRegExpPositionWithFlagDescriptor.FACTORY);
        fc.addGenerated(StringRegExpReplaceDescriptor.FACTORY);
        fc.addGenerated(StringRegExpReplaceWithFlagsDescriptor.FACTORY);
        fc.addGenerated(StringInitCapDescriptor.FACTORY);
        fc.addGenerated(StringTrimDescriptor.FACTORY);
        fc.addGenerated(StringLTrimDescriptor.FACTORY);
        fc.addGenerated(StringRTrimDescriptor.FACTORY);
        fc.addGenerated(StringTrim2Descriptor.FACTORY);
        fc.addGenerated(StringLTrim2Descriptor.FACTORY);
        fc.addGenerated(StringRTrim2Descriptor.FACTORY);
        fc.addGenerated(StringPositionDescriptor.FACTORY);
        fc.addGenerated(StringRepeatDescriptor.FACTORY);
        fc.addGenerated(StringReplaceDescriptor.FACTORY);
        fc.addGenerated(StringReplaceWithLimitDescriptor.FACTORY);
        fc.addGenerated(StringReverseDescriptor.FACTORY);
        fc.addGenerated(StringSplitDescriptor.FACTORY);

        // Constructors
        fc.addGenerated(ABooleanConstructorDescriptor.FACTORY);
        fc.addGenerated(ABinaryHexStringConstructorDescriptor.FACTORY);
        fc.addGenerated(ABinaryBase64StringConstructorDescriptor.FACTORY);
        fc.addGenerated(AStringConstructorDescriptor.FACTORY);
        fc.addGenerated(AInt8ConstructorDescriptor.FACTORY);
        fc.addGenerated(AInt16ConstructorDescriptor.FACTORY);
        fc.addGenerated(AInt32ConstructorDescriptor.FACTORY);
        fc.addGenerated(AInt64ConstructorDescriptor.FACTORY);
        fc.addGenerated(AFloatConstructorDescriptor.FACTORY);
        fc.addGenerated(ADoubleConstructorDescriptor.FACTORY);
        fc.addGenerated(APointConstructorDescriptor.FACTORY);
        fc.addGenerated(APoint3DConstructorDescriptor.FACTORY);
        fc.addGenerated(ALineConstructorDescriptor.FACTORY);
        fc.addGenerated(APolygonConstructorDescriptor.FACTORY);
        fc.addGenerated(ACircleConstructorDescriptor.FACTORY);
        fc.addGenerated(ARectangleConstructorDescriptor.FACTORY);
        fc.addGenerated(ATimeConstructorDescriptor.FACTORY);
        fc.addGenerated(ADateConstructorDescriptor.FACTORY);
        fc.addGenerated(ADateTimeConstructorDescriptor.FACTORY);
        fc.addGenerated(ADurationConstructorDescriptor.FACTORY);
        fc.addGenerated(AYearMonthDurationConstructorDescriptor.FACTORY);
        fc.addGenerated(ADayTimeDurationConstructorDescriptor.FACTORY);
        fc.addGenerated(AUUIDFromStringConstructorDescriptor.FACTORY);
        fc.addGenerated(AIntervalConstructorDescriptor.FACTORY);
        fc.addGenerated(AIntervalStartFromDateConstructorDescriptor.FACTORY);
        fc.addGenerated(AIntervalStartFromDateTimeConstructorDescriptor.FACTORY);
        fc.addGenerated(AIntervalStartFromTimeConstructorDescriptor.FACTORY);

        // Spatial
        fc.addGenerated(CreatePointDescriptor.FACTORY);
        fc.addGenerated(CreateLineDescriptor.FACTORY);
        fc.addGenerated(CreatePolygonDescriptor.FACTORY);
        fc.addGenerated(CreateCircleDescriptor.FACTORY);
        fc.addGenerated(CreateRectangleDescriptor.FACTORY);
        fc.addGenerated(SpatialAreaDescriptor.FACTORY);
        fc.addGenerated(SpatialDistanceDescriptor.FACTORY);
        fc.addGenerated(CreateMBRDescriptor.FACTORY);
        fc.addGenerated(SpatialCellDescriptor.FACTORY);
        fc.addGenerated(PointXCoordinateAccessor.FACTORY);
        fc.addGenerated(PointYCoordinateAccessor.FACTORY);
        fc.addGenerated(CircleRadiusAccessor.FACTORY);
        fc.addGenerated(CircleCenterAccessor.FACTORY);
        fc.addGenerated(LineRectanglePolygonAccessor.FACTORY);

        // full-text function
        fc.addGenerated(FullTextContainsDescriptor.FACTORY);
        fc.addGenerated(FullTextContainsWithoutOptionDescriptor.FACTORY);

        // Record functions.
        fc.addGenerated(GetRecordFieldsDescriptor.FACTORY);
        fc.addGenerated(GetRecordFieldValueDescriptor.FACTORY);
        fc.addGenerated(DeepEqualityDescriptor.FACTORY);
        fc.addGenerated(RecordMergeDescriptor.FACTORY);
        fc.addGenerated(RecordAddFieldsDescriptor.FACTORY);
        fc.addGenerated(RecordRemoveFieldsDescriptor.FACTORY);
        fc.addGenerated(RecordLengthDescriptor.FACTORY);
        fc.addGenerated(RecordNamesDescriptor.FACTORY);
        fc.addGenerated(RecordRemoveDescriptor.FACTORY);
        fc.addGenerated(RecordRenameDescriptor.FACTORY);
        fc.addGenerated(RecordUnwrapDescriptor.FACTORY);
        fc.add(RecordReplaceDescriptor.FACTORY);
        fc.add(RecordAddDescriptor.FACTORY);
        fc.add(RecordPutDescriptor.FACTORY);

        // Spatial and temporal type accessors
        fc.addGenerated(TemporalYearAccessor.FACTORY);
        fc.addGenerated(TemporalMonthAccessor.FACTORY);
        fc.addGenerated(TemporalDayAccessor.FACTORY);
        fc.addGenerated(TemporalHourAccessor.FACTORY);
        fc.addGenerated(TemporalMinuteAccessor.FACTORY);
        fc.addGenerated(TemporalSecondAccessor.FACTORY);
        fc.addGenerated(TemporalMillisecondAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalStartAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalEndAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalStartDateAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalEndDateAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalStartTimeAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalEndTimeAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalStartDatetimeAccessor.FACTORY);
        fc.addGenerated(TemporalIntervalEndDatetimeAccessor.FACTORY);

        // Temporal functions
        fc.addGenerated(UnixTimeFromDateInDaysDescriptor.FACTORY);
        fc.addGenerated(UnixTimeFromTimeInMsDescriptor.FACTORY);
        fc.addGenerated(UnixTimeFromDatetimeInMsDescriptor.FACTORY);
        fc.addGenerated(UnixTimeFromDatetimeInSecsDescriptor.FACTORY);
        fc.addGenerated(DateFromUnixTimeInDaysDescriptor.FACTORY);
        fc.addGenerated(DateFromDatetimeDescriptor.FACTORY);
        fc.addGenerated(TimeFromUnixTimeInMsDescriptor.FACTORY);
        fc.addGenerated(TimeFromDatetimeDescriptor.FACTORY);
        fc.addGenerated(DatetimeFromUnixTimeInMsDescriptor.FACTORY);
        fc.addGenerated(DatetimeFromUnixTimeInSecsDescriptor.FACTORY);
        fc.addGenerated(DatetimeFromDateAndTimeDescriptor.FACTORY);
        fc.addGenerated(CalendarDurationFromDateTimeDescriptor.FACTORY);
        fc.addGenerated(CalendarDuartionFromDateDescriptor.FACTORY);
        fc.addGenerated(AdjustDateTimeForTimeZoneDescriptor.FACTORY);
        fc.addGenerated(AdjustTimeForTimeZoneDescriptor.FACTORY);
        fc.addGenerated(IntervalBeforeDescriptor.FACTORY);
        fc.addGenerated(IntervalAfterDescriptor.FACTORY);
        fc.addGenerated(IntervalMeetsDescriptor.FACTORY);
        fc.addGenerated(IntervalMetByDescriptor.FACTORY);
        fc.addGenerated(IntervalOverlapsDescriptor.FACTORY);
        fc.addGenerated(IntervalOverlappedByDescriptor.FACTORY);
        fc.addGenerated(OverlapDescriptor.FACTORY);
        fc.addGenerated(IntervalStartsDescriptor.FACTORY);
        fc.addGenerated(IntervalStartedByDescriptor.FACTORY);
        fc.addGenerated(IntervalCoversDescriptor.FACTORY);
        fc.addGenerated(IntervalCoveredByDescriptor.FACTORY);
        fc.addGenerated(IntervalEndsDescriptor.FACTORY);
        fc.addGenerated(IntervalEndedByDescriptor.FACTORY);
        fc.addGenerated(DurationFromMillisecondsDescriptor.FACTORY);
        fc.addGenerated(DurationFromMonthsDescriptor.FACTORY);
        fc.addGenerated(YearMonthDurationGreaterThanComparatorDescriptor.FACTORY);
        fc.addGenerated(YearMonthDurationLessThanComparatorDescriptor.FACTORY);
        fc.addGenerated(DayTimeDurationGreaterThanComparatorDescriptor.FACTORY);
        fc.addGenerated(DayTimeDurationLessThanComparatorDescriptor.FACTORY);
        fc.addGenerated(MonthsFromYearMonthDurationDescriptor.FACTORY);
        fc.addGenerated(MillisecondsFromDayTimeDurationDescriptor.FACTORY);
        fc.addGenerated(DurationEqualDescriptor.FACTORY);
        fc.addGenerated(GetYearMonthDurationDescriptor.FACTORY);
        fc.addGenerated(GetDayTimeDurationDescriptor.FACTORY);
        fc.addGenerated(IntervalBinDescriptor.FACTORY);
        fc.addGenerated(OverlapBinsDescriptor.FACTORY);
        fc.addGenerated(DayOfWeekDescriptor.FACTORY);
        fc.addGenerated(ParseDateDescriptor.FACTORY);
        fc.addGenerated(ParseTimeDescriptor.FACTORY);
        fc.addGenerated(ParseDateTimeDescriptor.FACTORY);
        fc.addGenerated(PrintDateDescriptor.FACTORY);
        fc.addGenerated(PrintTimeDescriptor.FACTORY);
        fc.addGenerated(PrintDateTimeDescriptor.FACTORY);
        fc.addGenerated(GetOverlappingIntervalDescriptor.FACTORY);
        fc.addGenerated(DurationFromIntervalDescriptor.FACTORY);

        // Type functions.
        fc.addGenerated(IsArrayDescriptor.FACTORY);
        fc.addGenerated(IsAtomicDescriptor.FACTORY);
        fc.addGenerated(IsBooleanDescriptor.FACTORY);
        fc.addGenerated(IsNumberDescriptor.FACTORY);
        fc.addGenerated(IsObjectDescriptor.FACTORY);
        fc.addGenerated(IsStringDescriptor.FACTORY);
        fc.addGenerated(ToArrayDescriptor.FACTORY);
        fc.addGenerated(ToAtomicDescriptor.FACTORY);
        fc.addGenerated(ToBigIntDescriptor.FACTORY);
        fc.addGenerated(ToBooleanDescriptor.FACTORY);
        fc.addGenerated(ToDoubleDescriptor.FACTORY);
        fc.addGenerated(ToNumberDescriptor.FACTORY);
        fc.addGenerated(ToObjectDescriptor.FACTORY);
        fc.addGenerated(ToStringDescriptor.FACTORY);

        // Cast function
        fc.addGenerated(CastTypeDescriptor.FACTORY);
        fc.addGenerated(CastTypeLaxDescriptor.FACTORY);

        // Record function
        fc.addGenerated(RecordPairsDescriptor.FACTORY);

        // Other functions
        fc.addGenerated(RandomWithSeedDescriptor.FACTORY);

        ServiceLoader.load(IFunctionRegistrant.class).iterator().forEachRemaining(c -> c.register(fc));
        return fc;
    }

    public List<IFunctionDescriptorFactory> getFunctionDescriptorFactories() {
        return descriptorFactories;
    }

    /**
     * Gets the generated function descriptor factory from an <code>IFunctionDescriptor</code>
     * implementation class.
     *
     * @param cl,
     *            the class of an <code>IFunctionDescriptor</code> implementation.
     * @return the IFunctionDescriptorFactory instance defined in the class.
     */
    private static IFunctionDescriptorFactory getGeneratedFunctionDescriptorFactory(Class<?> cl) {
        try {
            String className =
                    CodeGenHelper.getGeneratedClassName(cl.getName(), CodeGenHelper.DEFAULT_SUFFIX_FOR_GENERATED_CLASS);
            Class<?> generatedCl = cl.getClassLoader().loadClass(className);
            Field factory = generatedCl.getDeclaredField(FACTORY);
            return (IFunctionDescriptorFactory) factory.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
