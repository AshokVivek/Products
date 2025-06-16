# Bank Connect Lambdas
This includes the lambdas used by Bank Connect product (also referred to as FinBox Statement Miner (FSM))

## Initial Setup
- Clone the repo locally
- Make sure serverless is installed as a global npm package
```sh
npm install -g serverless
```
- Now clone the sub module for the first time
```sh
git submodule init
git submodule update --remote
```


## Deploy
1. First make sure AWS profiles for dev and/or prod accounts are configured on your machine. It can be added using aws cli by `aws configure --profile <profile_name>` command. Refer to [this](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) page for detailed explanation.

2. Once configured switch to required AWS credentials profile on terminal, for example for profile "dev", use the following command (default profile is "default" if you don't run the command):
    ```sh
    export AWS_PROFILE=dev
    ```

3. After changing AWS profile locally:
    To deploy in dev, execute:
    ```sh
    serverless deploy --config serverless_dev.yml
    ```
    To deploy in prod, execute:
    ```sh
    serverless deploy --config serverless_prod.yml
    ```

## Deploying via the Makefile
* Trigger the following command specifying the AWS profile, which encapsulates the combination of the `serverless deploy` and the `aws lambda udpate-function-configuration` commands after ensuring that the version of AWS CLI is the latest (https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html):
    To deploy, execute:
    ```sh
    make deploy_and_update_lambda aws_profile=default stage=dev region=ap-south-1
    ```

## Steps to test and dockerized lambdas
1. First make sure that you have edited the DockerFile properly.
    - Note: The command handler at the DockerFile is meant to be overridden. It is meant for testing purposes only.

2. Make sure your docker is using virtualization framework and using Rosetta for x86/amd64 emulation. Although, the first batch of lamdas are arm64, if the old lambdas are to migrate, we need this.

3. Build the image by the following command:
    ```
    sudo docker build -t score_and_extraction --platform linux/arm64 .
    ```

4. Test the image locally by the following commands:
    - Run the container by:
    ```
    sudo docker run -p 9000:8080 score_and_extraction
    ```

    - Test by:
    ```
    curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"entity_id":"some_entity_id"}'
    ```


## Old Layer Lambda Setup Code Sample
    
    analyze_pdf:
        runtime: python3.8
        handler: python.handlers.analyze_pdf_handler
        role: arn:aws:iam::905031918257:role/bank-connect-lambdas-iam-role-prod
        memorySize: 1024
        timeout: 120
        reservedConcurrency: 30

        layers:
          - arn:aws:lambda:ap-south-1:905031918257:layer:BankConnectPackages-All-1-Python-3_8:2
          - arn:aws:lambda:ap-south-1:905031918257:layer:BankConnectPackages-Pandas-Python-3_8:2

        tags:
          Name: ${self:provider.stackTags.Pipeline}-${self:provider.stackTags.Stage}-analyze_pdf
    
    
## To Generate Wheel File
    -> cd local_wheels/category
    -> python setup.py bdist_wheel --universal
    -> Copy wheel from local_wheels/category/dist/category-0.0.1-py2.py3-none-any.whl to local_wheels/category-0.0.1-py2.py3-none-any.whl


## Serverless deployment process update description
- The purpose of this change is to ensure that plaintext secrets and values are not committed as part of the codebase
- [IMPORTANT] Ensure that the two lists: `SSM_PARAMETER_NAMES` and `SERVERLESS_PARAMETER_NAMES` are maintained parallely within the Makefile, ensuring correct ordering
- [IMPORTANT] All the new parameters which are to be added to fsm-lambdas are to be added to both these lists
- Process description is as follows:
    - Based on the stage, region and the parameter lists mentioned above, a Python script (fetch_ssm_parameters.py) is invoked from within a target `fetch_ssm_parameters_via_python`
    - The Python script does the following:
        - Reads all the above parameters via command line and parses them
        - Compares the lists to ensure that their lengths match
        - Creates a hashmap between the SSM parameter names and Serverless parameter names
        - Initializes the Boto SSM client
        - Since the Boto SSM client accepts only 10 parameters at a time, batching is done on the SSM parameter list
        - Each batch is sent over to a function which fetches their values in a single shot from AWS and prepares a concatenated list of the form: `--param=<key>=<value>`
        - All these concatenated lists from all batches are concatenated by the main function and the final string is returned to Makefile
    - The value which is recieved from the Python script (`--param=<key1>=<value1> --param=<key2>=<value2> --param=<key3>=<value3>`) is transformed in Make as:
        - Pattern substitution to replace `--param=<key1>=<value1>` with `--param="<key1>=<value1>"`
        - Since the values containing spaces are not parsed properly by Make and the above Pattern substitution does not work as intended, a string substitution is performed to replace the whitespace delimiter with a space
    - This concatenated value is passed back to the targets invoking the target `fetch_ssm_parameters_via_python` and then passed onto the serverless deploy command after echoing them once