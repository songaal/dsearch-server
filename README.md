## 소개
패스트캣X 서버를 소개합니다.



## 사전
사전의 사용 컬럼, 색인시 필요한 정보는 setting 인덱스에 기록이 됩니다.    
사전의 데이터는 data 인덱스에 관리하게 됩니다.  
인덱스의 명칭은  application.yml에서 명칭을 변경 할 수 있습니다.
패스트캣X 서버를 실행하게 되면 엘라스틱서치에 인덱스를 자동으로 생성합니다.
- 설정 인덱스명: .fastcatx_dict_setting
- 데이터 인덱스명: .fastcatx_dictionary

### 사전설정
사전 설정 추가 방법은 아래 내용을 참고하세요.   

#### 필수 필드값   

|필드|설명|값|
|---|---|---|
|id|사전 아이디| |
|name|사전 명| |
|type|UI 타입| (SET, SYNONYM, SPACE, COMPOUND, SYNONYM_2WAY, CUSTOM) 택 1 |
|tokenType|토큰 타입| (NONE, MAX, MIN, MID, HIGH) 택 1 |
|ignoreCase|대소문자 구분| (true, false) 택 1 |
|columns|사용할 컬럼| (id, keyword, value) 컬럼은 3가지로 고정 |
 
#### 설정 등록 예제

```
POST /.fastcatx_dict_setting/_doc
{
  "id" : "ENGLISH",
  "name" : "영단어 사전",
  "type" : "SET",
  "tokenType" : "MAX",
  "ignoreCase" : "true",
  "columns" : [
    {
      "type" : "keyword",
      "label" : "키워드"
    }
  ]
}
```


### 사전데이터  
설정 인덱스의 columns 필드를 기반으로 데이터를 사용합니다. 예를 들어 설정인덱스에 keyword만 등록 되어있고, 데이터에 keyword, value 필드에 값을 추가하면 value 필드는 무시하게 됩니다.   
사전 데이터 추가 방법은 아래 내용을 참고하세요. 

|필드|설명|
|---|---|
|id|keyword 타입|
|keyword|keyword 타입|
|value|,(콤마)로 구분함.|
|type|[필수값] 사전 아이디|


사전 데이터 등록 예제

```
POST /.fastcatx_dictionary/_doc
{
  "id" : "",
  "keyword" : "",
  "value" : "Hello,Hi"
  "type" :  "ENGLISH"
}
```