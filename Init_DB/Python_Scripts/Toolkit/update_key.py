import os
import sys
import time

src_loc = "../../"


def read_file(f_name: str) -> list[str]:
    file_contents: list[str] = []
    if os.path.exists(f_name):
        print(f"Found File {f_name}.")
        try:
            with open(f_name, 'r') as f:
                file_contents = f.readlines()
        except Exception as e:
            print(e.args[0])
            sys.exit()
        else:
            print("File read successfully.")
    else:
        print("File {} not Found. Exiting. ".format(f_name))
        sys.exit()
    return file_contents


def return_env_dict(loc_f_name: list[str]) -> dict:
    src_env_dict: dict = {}
    for line in loc_f_name:
        if not (line.startswith("#") or line.startswith('\n')):
            key = line.split("=")[0]
            try:
                value = line.split("=")[1].split("\"")[1]
            except:
                value = line.split("=")[1].split("\n")[0]
            src_env_dict[key] = value

    return src_env_dict


def read_tgt_file(loc_f_name: list[str]) -> dict:
    tgt_env_dict: dict = {}
    for i, line in enumerate(loc_f_name):
        key = line.strip()
        try:
            value = loc_f_name[i + 1].strip()
        except:
            break
        else:
            if key.startswith("- name:") and value.strip().startswith("value:"):
                k = key.split(":")[1].strip()
                try:
                    v = value.split("\"")[1].strip()
                except:
                    print("Exception raised for ", loc_f_name[i + 1].strip())
                tgt_env_dict[k] = v
    return tgt_env_dict


def create_req_envs(env_file_name: str) -> None:
    env_f_contents = read_file(f_name=src_loc+env_file_name)
    env_dict = return_env_dict(env_f_contents)
    required_envs = []
    print("Reading the Keys from the required environment variables.`")
    for key, _ in env_dict.items():
        if key.__contains__("TBL") or key.__contains__("SCHEMA") or key.__contains__("DB") or key.__contains__("VER"):
            required_envs.append(key)
    try:
        with open("required_envs.txt", 'w+') as f:
            for key in required_envs:
                f.writelines(key+"\n")
    except Exception as e:
        print(e.args[0])
    else:
        print("Written Required Environment variables to file.")


def check_req_envs(req_envs_file: str) ->None:
    status = True
    env_freq_envs = read_file(f_name=src_loc+req_envs_file)
    for x in env_freq_envs:
        if x not in os.environ.keys():
            print("Environment variable ",x, "missing.")
    if not status:
        print("Important Environment Variables are missing. Exiting...")
        sys.exit()
    

def main():
    src_file = read_file(src_loc + "aws_common.env")
    src_dict = return_env_dict(src_file)
    print()
    for config_files in os.listdir(src_loc):
        if config_files.startswith("k8") and config_files.endswith(".yaml"):
            print()
            print("*" * 20)
            tgt_dict = read_tgt_file(read_file(src_loc + config_files))
            print()
            print("Missing key_value pair for {} is :".format(src_loc + config_files))
            for key in src_dict.keys():
                if key.strip() not in (tgt_dict.keys()) and key.strip() not in ['POSTGRES_PASSWORD', 'AWS_ACCESS_KEY',
                                                                                'AWS_SECRET_KEY', "EMAIL_PASSWORD"]:
                    print("                - name: ", key)
                    print("                  value: ", "\"" + src_dict[key] + "\"")
            print("*" * 20)
            time.sleep(2)


if __name__ == "__main__":
    main()
