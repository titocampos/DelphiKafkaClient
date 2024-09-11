unit CRMStopWatch;

interface

uses
  Windows, SysUtils;

type
  TCRMStopWatch = class
  private
    FStartTick: DWORD;
    FStopTick: DWORD;
    FRunning: Boolean;
    function GetIsRunning: Boolean;
  public
    procedure Start;
    procedure Stop;
    function ElapsedMilliseconds: DWORD;
    constructor Create;
    property IsRunning: Boolean read GetIsRunning;
  end;

implementation

constructor TCRMStopWatch.Create;
begin
  inherited Create;
  FRunning := False;
  FStartTick := 0;
  FStopTick := 0;
end;

procedure TCRMStopWatch.Start;
begin
  FStartTick := GetTickCount;
  FRunning := True;
end;

procedure TCRMStopWatch.Stop;
begin
  if FRunning then
  begin
    FStopTick := GetTickCount;
    FRunning := False;
  end;
end;

function TCRMStopWatch.ElapsedMilliseconds: DWORD;
begin
  if FRunning then
    Result := GetTickCount - FStartTick
  else
    Result := FStopTick - FStartTick;
end;

function TCRMStopWatch.GetIsRunning: Boolean;
begin
  Result := FRunning;
end;

end.

