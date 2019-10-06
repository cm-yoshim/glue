## 概要
GlueのPythonシェルジョブで、whlファイルを参照するJOBを作成するまでの手順です。  
[こちら](https://docs.aws.amazon.com/en_pv/glue/latest/dg/add-job-python#create-python-extra-library)を参考にしています。


## 流れ
- 1.ローカルで「whlファイル」、「GlueJobで実行するスクリプト」を作成
- 2.上記2ファイルをS3にアップロード
- 3.上記2ファイルを利用したGlueJobを作成


## 手順
### 1.whlファイルを作成する
「use_whl」直下で下記のコマンドを実行する。  
挙動としては、「setup.py」に記述されている内容に基づいてwhlファイルを作成している。  
もし利用したいパッケージを追加したい場合は「setup.py」の「REQUIRED」に追記していく。  

```shell
$ python3 setup.py bdist_wheel
```

すると、下記のようなディレクトリ構成になる。  
「dist」配下の「yoshim_test_wheel-0.1.0-py3-none-any.whl」が今回利用するwhlファイル。

```
.
├── README.md
├── build
│   ├── bdist.macosx-10.13-x86_64
│   └── lib
│       └── src
│           ├── __init__.py
│           └── my_func.py
├── dist
│   └── yoshim_test_wheel-0.1.0-py3-none-any.whl
├── glue_script
│   └── glue_script.py
├── setup.py
├── src
│   ├── __init__.py
│   └── my_func.py
└── yoshim_test_wheel.egg-info
    ├── PKG-INFO
    ├── SOURCES.txt
    ├── dependency_links.txt
    ├── requires.txt
    └── top_level.txt
```

### 2.whlファイルをS3にアップロード
上記手順で作成したwhlファイルをGlueJobで利用するために、一旦S3にアップロードします。  


```shell
$ aws s3 cp ./dist/yoshim_test_wheel-0.1.0-py3-none-any.whl \
s3://<your-bucket>/glue/blog_whl/whl_file/ \
--profile <your-profile>
```

### 3.GlueJobで実行するスクリプトをS3にアップロード
上記でアップロードしたwhlファイルを参照するスクリプトをS3にアップロードします。  

```shell
$ aws s3 cp ./glue_script/glue_script.py \
s3://<your-bucket>/glue/blog_whl/script/ \
--profile <your-profile>
```

### 4.GlueJobの作成
手順「2」、「3」でS3にアップロードしたファイルを利用したGlueJobを作成する。  


```shell
$ aws glue create-job --name cm_yoshim_test \
--role <GlueJobに適応するIAMロールのARN> \
--command '{"Name" :  "pythonshell", "ScriptLocation" : "s3://<your-bucket>/glue/blog_whl/script/glue_script.py", "PythonVersion": "3"}' \
--default-arguments '{"--extra-py-files" : "s3://<your-bucket>/glue/blog_whl/whl_file/yoshim_test_wheel-0.1.0-py3-none-any.whl"}' \
--glue-version '1.0' \
--max-capacity 1 \
--profile <your-profile>
```

### 5.実行
上記のJobがちゃんと挙動するか、実行して確認する。  

```shell
$ aws glue start-job-run \
--job-name cm_yoshim_test \
--profile <your-profile>
```