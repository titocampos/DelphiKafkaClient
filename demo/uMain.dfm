object Form4: TForm4
  Left = -1487
  Top = 312
  Caption = 'Form4'
  ClientHeight = 493
  ClientWidth = 619
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  PixelsPerInch = 96
  TextHeight = 13
  object GroupBox1: TGroupBox
    Left = 8
    Top = 8
    Width = 596
    Height = 235
    Caption = 'Producer'
    TabOrder = 0
    object Label1: TLabel
      Left = 18
      Top = 18
      Width = 35
      Height = 13
      Caption = 'Config'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Tahoma'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object Label2: TLabel
      Left = 18
      Top = 64
      Width = 62
      Height = 13
      Caption = 'Mensagem'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Tahoma'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object Label3: TLabel
      Left = 18
      Top = 206
      Width = 68
      Height = 13
      Caption = 'Num. Copias'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Tahoma'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object Button1: TButton
      Left = 508
      Top = 201
      Width = 75
      Height = 25
      Caption = 'Send'
      Enabled = False
      TabOrder = 0
      OnClick = Button1Click
    end
    object memConfig: TMemo
      Left = 18
      Top = 34
      Width = 495
      Height = 27
      Lines.Strings = (
        'memConfig')
      TabOrder = 1
    end
    object Button2: TButton
      Left = 516
      Top = 36
      Width = 75
      Height = 25
      Caption = 'Start'
      TabOrder = 2
      OnClick = Button2Click
    end
    object mensagem: TMemo
      Left = 18
      Top = 78
      Width = 571
      Height = 113
      Lines.Strings = (
        '{'
        '    "mensagem": "Ol'#225', mundo!",'
        '    "descri'#231#227'o": "Exemplo de JSON com caracteres UTF-8",'
        '    "dados": {'
        '        "nome": "Jos'#233'",'
        '        "idade": 30,'
        '        "cidade": "S'#227'o Paulo",'
        '        "pa'#237's": "Brasil",'
        '        "s'#237'mbolos": "'#8364', '#165', '#163', $",'
        '    }'
        '}')
      ScrollBars = ssVertical
      TabOrder = 3
    end
    object Copias: TSpinEdit
      Left = 104
      Top = 197
      Width = 121
      Height = 22
      MaxValue = 50
      MinValue = 1
      TabOrder = 4
      Value = 0
    end
  end
  object GroupBox2: TGroupBox
    Left = 18
    Top = 274
    Width = 576
    Height = 187
    Caption = 'Producer'
    TabOrder = 1
    object Button3: TButton
      Left = 486
      Top = 159
      Width = 75
      Height = 25
      Caption = 'Button1'
      TabOrder = 0
      OnClick = Button1Click
    end
  end
end
