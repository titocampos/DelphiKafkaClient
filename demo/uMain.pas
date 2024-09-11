unit uMain;

interface

uses
  CRM.Types, Windows, Messages, SysUtils, Variants, Classes, Graphics, Controls, Forms, Dialogs,
  StdCtrls, ExtCtrls, Spin, ActnList, DateUtils,
  Kafka.Lib, Kafka.Factory, Kafka.Interfaces, Kafka.Helper, Kafka.Types;

type
  TForm4 = class(TForm)
    GroupBox1: TGroupBox;
    Button1: TButton;
    GroupBox2: TGroupBox;
    Button3: TButton;
    Label1: TLabel;
    memConfig: TMemo;
    Button2: TButton;
    mensagem: TMemo;
    Label2: TLabel;
    Label3: TLabel;
    Copias: TSpinEdit;
    procedure Button1Click(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure Button2Click(Sender: TObject);
  private
    { Private declarations }
    FKafkaProducer: IKafkaProducer;
  public
    { Public declarations }
  end;

var
  Form4: TForm4;

implementation

{$R *.dfm}

procedure AddItemToStringArray(var Arr: TStringArray; const Item: string);
begin
  SetLength(Arr, Length(Arr) + 1);
  Arr[High(Arr)] := Item;
end;

procedure TForm4.Button1Click(Sender: TObject);
var
  Msgs: TStringArray;
  i: Integer;
begin
  if FKafkaProducer = nil then
    Exit;

  SetLength(Msgs, Copias.Value);

  for i := 0 to Length(Msgs) - 1 do
    Msgs[i] := mensagem.Text;

  FKafkaProducer.Produce(
    'meu_teste',
    Msgs,
    '',
    0,
    RD_KAFKA_MSG_F_COPY,
    nil);

  TKafkaHelper.Flush(FKafkaProducer.KafkaHandle);
end;

procedure TForm4.Button2Click(Sender: TObject);
var
  Msgs, Names, Values: TStringArray;
begin
  if FKafkaProducer = nil then
  begin
    TKafkaUtils.StringsToConfigArrays(memConfig.Lines, Names, Values);
    FKafkaProducer := TKafkaFactory.NewProducer(Names,Values);
  end;
  Button1.Enabled := True;
end;

procedure TForm4.FormCreate(Sender: TObject);
begin
  memConfig.Text := 'bootstrap.servers=10.24.24.9:29092';
end;

end.
