unit Kafka.Helper;

interface

uses
  CRM.Types, CRMStopWatch, SysUtils, Classes, DateUtils, SyncObjs, Kafka.Types, Kafka.Lib;

type
  EKafkaError = class(Exception);

  TOnLog = procedure(const Values: TStrings) of object;

  TOperationTimer = class
  private
    FStopwatch: TCRMStopWatch;
    FOperation: String;
  public
    constructor Create(const Operation: String); overload;
    constructor Create(const Operation: String; Args: Array of const); overload;
    destructor Destroy; override;
    procedure Start(const Operation: String); overload;
    procedure Start(const Operation: String; Args: Array of const); overload;
    procedure Stop;
    class procedure Finalize(var ADest: TOperationTimer);
  end;

  TKafkaUtils = class
  public
    class function PointerToStr(const Value: Pointer; const Len: Integer): String; static;
    class function PointerToBytes(const Value: Pointer; const Len: Integer): TBytes; static;
    class function StrToBytes(const Value: String): TBytes; static;
    class procedure StringsToConfigArrays(const Values: TStrings; out NameArr, ValueArr: TStringArray); static;
    class function StringsToIntegerArray(const Value: String): TIntegerArray;
    class function DateTimeToStrMS(const Value: TDateTime): String; static;
  end;

  TKafkaHelper = class
  private
    class var FLogStrings: TStringList;
    class var FCriticalSection: TCriticalSection;
    class var FOnLog: TOnLog;

    class procedure CheckKeyValues(const Keys, Values: TStringArray); static;
  protected
    class procedure DoLog(const Text: String; const LogType: TKafkaLogType);
  public
    class procedure Initialize;
    class procedure Finalize;

    class procedure Log(const Text: String; const LogType: TKafkaLogType);

    // Wrappers
    class function NewConfiguration(const DefaultCallBacks: Boolean = True): prd_kafka_conf_t; overload; static;
    class function NewConfiguration(const Keys, Values: TStringArray; const DefaultCallBacks: Boolean = True): prd_kafka_conf_t; overload; static;
    class procedure SetConfigurationValue(var Configuration: prd_kafka_conf_t; const Key, Value: String); static;
    class procedure DestroyConfiguration(const Configuration: Prd_kafka_conf_t); static;

    class function NewTopicConfiguration: prd_kafka_topic_conf_t; overload; static;
    class function NewTopicConfiguration(const Keys, Values: TStringArray): prd_kafka_topic_conf_t; overload; static;
    class procedure SetTopicConfigurationValue(var TopicConfiguration: prd_kafka_topic_conf_t; const Key, Value: String); static;
    class procedure DestroyTopicConfiguration(const TopicConfiguration: Prd_kafka_topic_conf_t); static;

    class function NewProducer(const Configuration: prd_kafka_conf_t): prd_kafka_t; overload; static;
    class function NewProducer(const ConfigKeys, ConfigValues: TStringArray): prd_kafka_t; overload; static;

    class function NewConsumer(const Configuration: prd_kafka_conf_t): prd_kafka_t; overload; static;

    class procedure ConsumerClose(const KafkaHandle: prd_kafka_t); static;
    class procedure DestroyHandle(const KafkaHandle: prd_kafka_t); static;

    class function NewTopic(const KafkaHandle: prd_kafka_t; const TopicName: UTF8String; const TopicConfiguration: prd_kafka_topic_conf_t = nil): prd_kafka_topic_t;

    class function Produce(const Topic: prd_kafka_topic_t; const Payload: Pointer; const PayloadLength: NativeUInt; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payloads: TPointerArray; const PayloadLengths: TIntegerArray; const Key: Pointer = nil; const KeyLen: NativeUInt = 0; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;
    class function Produce(const Topic: prd_kafka_topic_t; const Payloads: TStringArray; const Key: String; const Partition: Int32 = RD_KAFKA_PARTITION_UA; const MsgFlags: Integer = RD_KAFKA_MSG_F_COPY; const MsgOpaque: Pointer = nil): Integer; overload;

    class procedure Flush(const KafkaHandle: prd_kafka_t; const Timeout: Integer = 1000);

    // Utils
    class function IsKafkaError(const Error: rd_kafka_resp_err_t): Boolean; static;

    // Internal
    class procedure FlushLogs;

    class property OnLog: TOnLog read FOnLog write FOnLog;
  end;

implementation

resourcestring
  StrLogCallBackFac = 'Log_CallBack - fac = %s, buff = %s';
  StrMessageSendResult = 'Message send result = %d';
  StrErrorCallBackReaso = 'Error =  %s';
  StrUnableToCreateKaf = 'Unable to create Kafka Handle - %s';
  StrKeysAndValuesMust = 'Keys and Values must be the same length';
  StrMessageNotQueued = 'Message not Queued';
  StrInvalidConfiguratio = 'Invalid configuration key';
  StrInvalidTopicConfig = 'Invalid topic configuration key';
  StrCriticalError = 'Critical Error: ';

// Global callbacks

procedure ProducerCallBackLogger(rk: prd_kafka_t; rkmessage: prd_kafka_message_t;
  opaque: Pointer); cdecl;
begin
  if rkmessage <> nil then
  begin
    TKafkaHelper.Log(format(StrMessageSendResult, [Integer(rkmessage.err)]), kltProducer);
  end;
end;

procedure LogCallBackLogger(rk: prd_kafka_t; level: integer; fac: PAnsiChar;
  buf: PAnsiChar); cdecl;
begin
  TKafkaHelper.Log(format(StrLogCallBackFac, [String(fac), String(buf)]), kltLog);
end;

procedure ErrorCallBackLogger(rk: prd_kafka_t; err: integer; reason: PAnsiChar;
  opaque: Pointer); cdecl;
begin
  TKafkaHelper.Log(format(StrErrorCallBackReaso, [String(reason)]), kltError);
end;

{ TOperationTimer }

constructor TOperationTimer.Create(const Operation: String);
begin
  FStopwatch := TCRMStopWatch.Create;
  Start(Operation);
end;

constructor TOperationTimer.Create(const Operation: String; Args: Array of const);
begin
  Create(format(Operation, Args));
end;

destructor TOperationTimer.Destroy;
begin
  FreeAndNil(FStopwatch);
  inherited;
end;

procedure TOperationTimer.Start(const Operation: String);
begin
  Stop;

  FOperation := Operation;

  TKafkaHelper.Log('Started - ' + FOperation, kltDebug);

  FStopwatch.Start;
end;

procedure TOperationTimer.Start(const Operation: String; Args: Array of const);
begin
  Start(format(Operation, Args));
end;

procedure TOperationTimer.Stop;
begin
  if FStopwatch.IsRunning then
  begin
    FStopwatch.Stop;

    TKafkaHelper.Log(Format('Finished - %s in %d ms', [FOperation, FStopwatch.ElapsedMilliseconds]), kltDebug);
  end;
end;

class procedure TOperationTimer.Finalize(var ADest: TOperationTimer);
begin
  if ADest.FStopwatch.IsRunning then
  begin
    ADest.FStopwatch.Stop;
    TKafkaHelper.Log(Format('Finished - %s in %d ms', [ADest.FOperation, ADest.FStopwatch.ElapsedMilliseconds]), kltDebug);
  end;
end;

{ TKafkaUtils }

class procedure TKafkaUtils.StringsToConfigArrays(const Values: TStrings; out NameArr, ValueArr: TStringArray);
var
  i: Integer;
  Key, Value: string;
  EqualPos: Integer;
begin
  SetLength(NameArr, 0);
  SetLength(ValueArr, 0);

  for i := 0 to Values.Count - 1 do
  begin
    EqualPos := Pos('=', Values[i]);
    if EqualPos > 0 then
    begin
      Key := Copy(Values[i], 1, EqualPos - 1);
      Value := Copy(Values[i], EqualPos + 1, MaxInt);

      SetLength(NameArr, Length(NameArr) + 1);
      NameArr[High(NameArr)] := Key;

      SetLength(ValueArr, Length(ValueArr) + 1);
      ValueArr[High(ValueArr)] := Value;
    end;
  end;
end;

class function TKafkaUtils.StringsToIntegerArray(const Value: String): TIntegerArray;
var
  StrArray: TStringList;
  i: Integer;
begin
  StrArray := TStringList.Create;
  try
    StrArray.CommaText := Value;  // Usa TStringList para dividir a string
    SetLength(Result, StrArray.Count);

    for i := 0 to StrArray.Count - 1 do
    begin
      Result[i] := StrToInt(StrArray[i]);
    end;
  finally
    StrArray.Free;
  end;
end;

class function TKafkaUtils.DateTimeToStrMS(const Value: TDateTime): String;
begin
  Result := DateTimeToStr(Value) + '.' + FormatFloat('000', MilliSecondOf(Value));
end;


class function TKafkaUtils.StrToBytes(const Value: String): TBytes;
var
  UTF8Str: UTF8String;
  i: Integer;
begin
  if Value = '' then
    SetLength(Result, 0)
  else
  begin
    UTF8Str := UTF8Encode(Value);;

    SetLength(Result, Length(UTF8Str));
    for i := 1 to Length(UTF8Str) do
      Result[i - 1] := Byte(UTF8Str[i]);
  end;
end;

class function TKafkaUtils.PointerToStr(const Value: Pointer; const Len: Integer): String;
var
  Data: TBytes;
  AnsiStr: AnsiString;
//  UTF8Str: UTF8String;
begin
  SetLength(Data, Len);
  Move(Value^, Pointer(Data)^, Len);

//  case Encoding of
//    etANSI:
//      begin
        SetString(AnsiStr, PAnsiChar(Value), Len);
        Result := String(AnsiStr);
(*/
      end;
    etUTF8:
      begin
        SetString(UTF8Str, PAnsiChar(Value), Len);
        Result := UTF8Decode(UTF8Str);
      end;
    else
      Result := '';
  end;
*)  
end;

class function TKafkaUtils.PointerToBytes(const Value: Pointer; const Len: Integer): TBytes;
begin
  SetLength(Result, Len);
  Move(Value^, Pointer(Result)^, Len);
end;


{ TKafkaHelper }

class procedure TKafkaHelper.ConsumerClose(const KafkaHandle: prd_kafka_t);
begin
  rd_kafka_consumer_close(KafkaHandle);
end;

class procedure TKafkaHelper.DestroyHandle(const KafkaHandle: prd_kafka_t);
begin
  rd_kafka_destroy(KafkaHandle);
end;

class procedure TKafkaHelper.DoLog(const Text: String; const LogType: TKafkaLogType);
begin
  FCriticalSection.Enter;
  try
    TKafkaHelper.FLogStrings.AddObject(Text, TObject(LogType));
  finally
    FCriticalSection.Leave;
  end;
end;

class procedure TKafkaHelper.Initialize;
begin
  FLogStrings := TStringList.Create;
  FCriticalSection := TCriticalSection.Create;
end;

class procedure TKafkaHelper.Finalize;
begin
  FreeAndNil(FLogStrings);
  FreeAndNil(FCriticalSection);
end;

class procedure TKafkaHelper.Flush(const KafkaHandle: prd_kafka_t; const Timeout: Integer);
begin
  TOperationTimer.Create('Flushing');

  rd_kafka_flush(KafkaHandle, Timeout);
end;

class procedure TKafkaHelper.FlushLogs;
begin
  FCriticalSection.Enter;
  try
    if Assigned(FOnLog) then
      FOnLog(TKafkaHelper.FLogStrings);

    TKafkaHelper.FLogStrings.Clear;
  finally
    FCriticalSection.Leave;
  end;
end;

class procedure TKafkaHelper.Log(const Text: String; const LogType: TKafkaLogType);
begin
  DoLog(Text, LogType);
end;

class procedure TKafkaHelper.DestroyConfiguration(const Configuration: Prd_kafka_conf_t);
begin
  rd_kafka_conf_destroy(Configuration);
end;

class procedure TKafkaHelper.DestroyTopicConfiguration(const TopicConfiguration: Prd_kafka_topic_conf_t);
begin
  rd_kafka_topic_conf_destroy(TopicConfiguration);
end;

class function TKafkaHelper.NewConfiguration(const Keys: TStringArray; const Values: TStringArray; const DefaultCallBacks: Boolean): prd_kafka_conf_t;
var
  i: Integer;
begin
  CheckKeyValues(Keys, Values);

  Result := rd_kafka_conf_new();

  for i := Low(keys) to High(Keys) do
  begin
    SetConfigurationValue(
      Result,
      Keys[i],
      Values[i]);
  end;

  if DefaultCallBacks then
  begin
    rd_kafka_conf_set_dr_msg_cb(Result, @ProducerCallBackLogger);
    rd_kafka_conf_set_log_cb(Result, @LogCallBackLogger);
    rd_kafka_conf_set_error_cb(Result, @ErrorCallBackLogger);
  end;
end;

class function TKafkaHelper.NewProducer(const ConfigKeys, ConfigValues: TStringArray): prd_kafka_t;
var
  Configuration: prd_kafka_conf_t;
begin
  Configuration := TKafkaHelper.NewConfiguration(
    ConfigKeys,
    ConfigValues);

  Result := NewProducer(Configuration);
end;

class function TKafkaHelper.NewConsumer(const Configuration: prd_kafka_conf_t): prd_kafka_t;
var
  ErrorStr: TKafkaErrorArray;
begin
  Result := rd_kafka_new(
    RD_KAFKA_CONSUMER,
    Configuration,
    ErrorStr,
    Sizeof(ErrorStr));

  if Result = nil then
  begin
    raise EKafkaError.CreateFmt(StrUnableToCreateKaf, [String(ErrorStr)]);
  end;
end;

class function TKafkaHelper.NewProducer(const Configuration: prd_kafka_conf_t): prd_kafka_t;
var
  ErrorStr: TKafkaErrorArray;
begin
  Result := rd_kafka_new(
    RD_KAFKA_PRODUCER,
    Configuration,
    ErrorStr,
    Sizeof(ErrorStr));

  if Result = nil then
  begin
    raise EKafkaError.CreateFmt(StrUnableToCreateKaf, [String(ErrorStr)]);
  end;
end;

class function TKafkaHelper.NewTopic(const KafkaHandle: prd_kafka_t; const TopicName: UTF8String; const TopicConfiguration: prd_kafka_topic_conf_t): prd_kafka_topic_t;
begin
  Result := rd_kafka_topic_new(
    KafkaHandle,
    PAnsiChar(AnsiString(TopicName)),
    TopicConfiguration);

  if Result = nil then
  begin
    raise EKafkaError.Create(String(rd_kafka_err2str(rd_kafka_last_error)));
  end;
end;

class function TKafkaHelper.NewTopicConfiguration: prd_kafka_topic_conf_t;
var
  EmptyArray : TStringArray;
begin
  SetLength(EmptyArray, 0);
  Result := NewTopicConfiguration(EmptyArray, EmptyArray);
end;

class procedure TKafkaHelper.CheckKeyValues(const Keys, Values: TStringArray);
begin
  if length(keys) <> length(values) then
  begin
    raise EKafkaError.Create(StrKeysAndValuesMust);
  end;
end;

class function TKafkaHelper.NewTopicConfiguration(const Keys, Values: TStringArray): prd_kafka_topic_conf_t;
var
  i: Integer;
begin
  Result := rd_kafka_topic_conf_new;

  CheckKeyValues(Keys, Values);

  for i := Low(keys) to High(Keys) do
  begin
    SetTopicConfigurationValue(
      Result,
      Keys[i],
      Values[i]);
  end;
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payloads: TPointerArray; const PayloadLengths: TIntegerArray; const Key: Pointer;
  const KeyLen: NativeUInt; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  i: Integer;
  Msgs: TArrayrd_kafka_message_t;
  Msg: rd_kafka_message_t;
begin
  if length(Payloads) = 0 then
    Result := 0
  else
  begin
    SetLength(Msgs, length(Payloads));

    for i := Low(Payloads) to High(Payloads) do
    begin
      Msg.partition := Partition;
      Msg.rkt := Topic;
      Msg.payload := Payloads[i];
      Msg.len := PayloadLengths[i];
      Msg.key := Key;
      Msg.key_len := KeyLen;

      Msgs[i] := Msg;
    end;

    Result := rd_kafka_produce_batch(
      Topic,
      Partition,
      MsgFlags,
      @Msgs[0],
      length(Payloads));

    if Result <> length(Payloads) then
    begin
      raise EKafkaError.Create(StrMessageNotQueued);
    end;
  end;
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payloads: TStringArray; const Key: String; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  PayloadPointers: TPointerArray;
  PayloadLengths: TIntegerArray;
  KeyBytes, PayloadBytes: TBytes;
  i: Integer;
  KeyData: TBytes;
begin
  SetLength(PayloadPointers, length(Payloads));
  SetLength(PayloadLengths, length(Payloads));

  KeyBytes := TKafkaUtils.StrToBytes(Key);

  for i := Low(Payloads) to High(Payloads) do
  begin
    PayloadBytes := TKafkaUtils.StrToBytes(Payloads[i]);

    PayloadPointers[i] := @PayloadBytes[0];
    PayloadLengths[i] := Length(PayloadBytes);
  end;

  Result := Produce(
    Topic,
    PayloadPointers,
    PayloadLengths,
    @KeyBytes[0],
    Length(KeyBytes),
    Partition,
    MsgFlags,
    MsgOpaque);
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payload: String; const Key: String; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
var
  KeyBytes, PayloadBytes: TBytes;
begin
  KeyBytes := TKafkaUtils.StrToBytes(Key);
  PayloadBytes := TKafkaUtils.StrToBytes(Payload);

  Result := Produce(
    Topic,
    @PayloadBytes[0],
    length(PayloadBytes),
    @KeyBytes[0],
    length(KeyBytes),
    Partition,
    MsgFlags,
    MsgOpaque)
end;

class function TKafkaHelper.Produce(const Topic: prd_kafka_topic_t; const Payload: Pointer; const PayloadLength: NativeUInt;
  const Key: Pointer; const KeyLen: NativeUInt; const Partition: Int32; const MsgFlags: Integer; const MsgOpaque: Pointer): Integer;
begin
  Result := rd_kafka_produce(
    Topic,
    Partition,
    MsgFlags,
    Payload,
    PayloadLength,
    Key,
    KeyLen,
    MsgOpaque);

  if Result = -1 then
  begin
    raise EKafkaError.Create(StrMessageNotQueued);
  end;
end;

class function TKafkaHelper.NewConfiguration(const DefaultCallBacks: Boolean): prd_kafka_conf_t;
var
  EmptyArray : TStringArray;
begin
  Result := NewConfiguration(EmptyArray, EmptyArray, DefaultCallBacks);
end;

class procedure TKafkaHelper.SetConfigurationValue(var Configuration: prd_kafka_conf_t; const Key, Value: String);
var
  ErrorStr: TKafkaErrorArray;
begin
  if Value = '' then
  begin
    raise EKafkaError.Create(StrInvalidConfiguratio);
  end;

  if rd_kafka_conf_set(
    Configuration,
    PAnsiChar(AnsiString(Key)),
    PAnsiChar(AnsiString(Value)),
    ErrorStr,
    Sizeof(ErrorStr)) <> RD_KAFKA_CONF_OK then
  begin
    raise EKafkaError.Create(String(ErrorStr));
  end;
end;

class procedure TKafkaHelper.SetTopicConfigurationValue(var TopicConfiguration: prd_kafka_topic_conf_t; const Key, Value: String);
var
  ErrorStr: TKafkaErrorArray;
begin
  if Value = '' then
  begin
    raise EKafkaError.Create(StrInvalidTopicConfig);
  end;

  if rd_kafka_topic_conf_set(
    TopicConfiguration,
    PAnsiChar(AnsiString(Key)),
    PAnsiChar(AnsiString(Value)),
    ErrorStr,
    Sizeof(ErrorStr)) <> RD_KAFKA_CONF_OK then
  begin
    raise EKafkaError.Create(String(ErrorStr));
  end;
end;

class function TKafkaHelper.IsKafkaError(const Error: rd_kafka_resp_err_t): Boolean;
begin
  Result :=
    (Error <> RD_KAFKA_RESP_ERR_NO_ERROR) and
    (Error <> RD_KAFKA_RESP_ERR__PARTITION_EOF);
end;

initialization
  TKafkaHelper.Initialize;

finalization
  TKafkaHelper.Finalize;

end.
