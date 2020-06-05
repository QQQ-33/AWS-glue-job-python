import boto3
import traceback
import sys
from os import environ
import logging
import json
import urllib.parse
import datetime
import time
from io import StringIO


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_input_bucket = "preventech-china-er-output"
s3_output_bucket = "preventech-china-er-format-output-chinastage"
templete_params = '''
[
    {
        "spn":"100",
        "name": "Engine Oil Pressure 1",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"110",
        "name": "Engine Coolant Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"156",
        "name": "Engine Fuel 1 Injector Timing Rail 1 Pressure",
        "unit": "MPa",
        "sourceAddr": 0 
    },
    {
        "spn":"22",
        "name": "Engine Extended Crankcase Blow-by Pressure",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"175",
        "name": "Engine Oil Temperature 1",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"102",
        "name": "Engine Intake Manifold #1 Pressure",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"1127",
        "name": "Engine Turbocharger 1 Boost Pressure",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"1128",
        "name": "Engine Turbocharger 2 Boost Pressure",
        "unit": "kPa",
        "sourceAddr": 0 ,
        "sourceAddr": 0 
    },
    {
        "spn":"1129",
        "name": "Engine Turbocharger 3 Boost Pressure",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"190",
        "name": "Engine Speed",
        "unit": "rpm",
        "sourceAddr": 0 
    },
    {
        "spn":"109",
        "name": "Engine Coolant Pressure 1",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"157",
        "name": "Engine Fuel 1 Injector Metering Rail 1 Pressure",
        "unit": "MPa",
        "sourceAddr": 0 
    },
    {
        "spn":"174",
        "name": "Engine Fuel 1 Temperature 1",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"101",
        "name": "Engine Crankcase Pressure 1",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"105",
        "name": "Engine Intake Manifold 1 Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"1131",
        "name": "Engine Intake Manifold 2 Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"1132",
        "name": "Engine Intake Manifold 3 Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"1133",
        "name": "Engine Intake Manifold 4 Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"7471",
        "name": "Engine Oil Filter Differential Pressure (Extended Range)",
        "unit": "kPa",
        "sourceAddr": 130 
    },
    {
        "spn":"91",
        "name": "Accelerator Pedal Position 1",
        "unit": "%",
        "sourceAddr": 0 
    },
    {
        "spn":"92",
        "name": "Engine Percent Load At Current Speed",
        "unit": "%",
        "sourceAddr": 0 
    },
    {
        "spn":"168",
        "name": "Battery Potential / Power Input 1",
        "unit": "V",
        "sourceAddr": 0 
    },
    {
        "spn":"108",
        "name": "Barometric Pressure",
        "unit": "kPa",
        "sourceAddr": 0 
    },
    {
        "spn":"580",
        "name": "Altitude",
        "unit": "m",
        "sourceAddr": 0 
    },
    {
        "spn":"173",
        "name": "Engine Exhaust Temperature",
        "unit": "°C",
        "sourceAddr": 0 
    },
    {
        "spn":"235",
        "name": "Engine Total Idle Hours",
        "unit": "h",
        "sourceAddr": 0 
    },
    {
        "spn":"250",
        "name": "Engine Total Fuel Used",
        "unit": "l",
        "sourceAddr": 0 
    },
    {
        "spn":"247",
        "name": "Engine Total Hours of Operation",
        "unit": "h",
        "sourceAddr": 0 
    },
    {
        "spn":"1033",
        "name": "Total ECU Run Time",
        "unit": "h",
        "sourceAddr": 0 
    }
]
'''

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    templete_params_json = json.loads(templete_params)
    # print(templete_params_json)
    # csv format data
    output_file_content = ""
    parameters_line = getTempleteLine(templete_params_json, "name")
    spn_line = getTempleteLine(templete_params_json, "spn")
    units_line = getTempleteLine(templete_params_json, "unit")
    # source_addr_line = getTempleteLine(templete_params_json, "sourceAddr")
    params_data_perfix = ","

    try:
        file_obj = event["Records"][0]
        filename = str(file_obj["s3"]["object"]["key"])
        filename = urllib.parse.unquote(filename)
        # print("filename : ", filename)
        fileObj = s3.get_object(Bucket=s3_input_bucket, Key=filename)
        file_content = fileObj["Body"].read().decode("utf-8").splitlines()
        csn = ""
        # process json data
        for record in file_content:
            temp_json = json.loads(record)
            csn = temp_json["componentserialnumber"]
            if output_file_content == "":
                output_file_content = ( output_file_content 
                + "Customer_Name," + temp_json["custname"] + "\n" 
                + "Group_Name," + temp_json["group_name"] + "\n"
                + "SubGroup_Name," + temp_json["subgroup_name"] + "\n"
                + "SubGroup_ID," + temp_json["subgroup_id"] + "\n"
                + "SMN," + temp_json["rel_smn"] + "\n"
                + "ESN," + csn + "\n"
                + "VIN," + temp_json["vin"] + "\n"
                + "ECM_Code," + temp_json["calibration"] + "\n"
                + "Nominal_torque," + temp_json["nominal_torque"] + "\n"
                + "Fuel_Rating," + temp_json["fuel_rating"] + "\n"
                + "Telematics_Device," + temp_json["telematicsdeviceid"] + "\n"
                + "Parameter Name,DLA Timestamp,Horse_Power," + parameters_line + "\n"
                + "SPN,,," + spn_line + "\n"
                + "Units,,," + units_line + "\n"
                # + "Source Address," + source_addr_line + "\n" 
                )
            # process spn params
            try:
                samples_str = temp_json["samples"]
                horse_power = temp_json["horse_power"]
                if samples_str != "":
                    samples_json = json.loads(samples_str)
                    parameters_list = samples_json[0]["convertedEquipmentParameters"]
                    data_timestamp = str(samples_json[0]["dateTimestamp"])
                    spn_value_line = params_data_perfix + data_timestamp + "," + horse_power + ","
                    for templete_spn in templete_params_json:
                        spn_value = getParamFromER(parameters_list, templete_spn["spn"])
                        spn_value_line = spn_value_line + spn_value + ","
                    output_file_content = output_file_content + spn_value_line + "\n"
                # print(output_file_content)    
            except:
                logger.error("ERROR: invailde data from er-data-json.\n{}".format(temp_json))
                continue
            
            # break
        # output csv data
        output_filename = getOutputFileName(filename, csn)
        print("writing csv file :" + output_filename)
        # print(output_file_content)
        return s3.put_object(Bucket= s3_output_bucket,
                             Body= output_file_content,
                             Key=output_filename
                             )

    except:
        return log_err("ERROR: Cannot connect to cache from handler.\n{}".format(
            traceback.format_exc()))

    finally:
        try:
            print('job done ......')
        except:
            pass

def getTempleteLine(data, lineName):
    line = ""
    for spn_obj in data:
        line = line + str(spn_obj[lineName]) + ","
    return line

def getParamFromER(paramList, paramName):
    param_value = ""
    for paramObj in paramList:
        # print(paramObj["parameters"][paramName])
        try:
            param_value = str(paramObj["parameters"][paramName])
        except:
            pass
    # print("spn:"+paramName + " value:" +param_value)    
    return param_value

def getOutputFileName(filepath, csn):
    pathlist = filepath.split("/")
    year = pathlist[3]
    month = pathlist[4]
    day = pathlist[5]
    pathlist.pop()
    pathlist.pop()
    
    return "/".join(pathlist) + "/" + csn + "_{}-{}-{}.csv".format(year, month, day)

def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg, "headers": {}, "statusCode": 400,
            "isBase64Encoded": "false"}