import os
import streamlit as st

def get_secret(key):
    try:
        return st.secrets[key]
    except:
        return os.environ.get(key)

GITHUB_API_KEY=get_secret("GITHUB_API_KEY")
OPENAI_API_KEY=get_secret("OPENAI_API_KEY")
GOOGLE_API_KEY=get_secret("GOOGLE_API_KEY")
LASTFM_API_KEY=get_secret("LASTFM_API_KEY")
FRIENDLI_API_KEY=get_secret("FRIENDLI_API_KEY")
AWS_ACCESS_KEY=get_secret("AWS_ACCESS_KEY")
AWS_SECRET_KEY=get_secret("AWS_SECRET_KEY")
AWS_REGION=get_secret("AWS_REGION")
S3_BUCKET_NAME=get_secret("S3_BUCKET_NAME")
S3_FILE_KEY=get_secret("S3_FILE_KEY")

PG_CONFIG={
    "user":get_secret("POSTGRES_USER") or get_secret("PG_USER") or "postgres",
    "password":get_secret("POSTGRES_PASSWORD") or get_secret("PG_PASSWORD") or "",
    "host":get_secret("POSTGRES_HOST") or get_secret("PG_HOST") or "",
    "port":str(get_secret("POSTGRES_PORT") or get_secret("PG_PORT") or "5432"),
    "database":get_secret("POSTGRES_DB") or get_secret("PG_DB") or "music",
}

SNOW_CONFIG={
    "user":get_secret("SNOWFLAKE_USER") or "",
    "password":get_secret("SNOWFLAKE_PASSWORD") or "",
    "account":get_secret("SNOWFLAKE_ACCOUNT") or "",
    "warehouse":get_secret("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH",
    "database":get_secret("SNOWFLAKE_DATABASE") or "MUSIC_DB",
    "schema":get_secret("SNOWFLAKE_SCHEMA") or "PUBLIC",
}

FOLDER_PATH=r"/home/azureuser/project1/cmdata" if os.path.exists("/home/azureuser/project1") else r"C:\Users\KimTaeRyong\Downloads\cmdata"

SIMON_CONFIG={
    "provider":"FRIENDLI",
    "models":{
        "GITHUB_LLAMA":{
            "model_id":"Llama-4-Scout-17B-16E-Instruct",
            "base_url":"https://models.inference.ai.azure.com",
            "api_key_name":"GITHUB_API_KEY"
        },
        "FRIENDLI":{
            "model_id":"meta-llama-3.1-8b-instruct",
            "base_url":"https://inference.friendli.ai/v1",
            "api_key_name":"FRIENDLI_API_KEY"
        }
    }
}

JUDGES={
    "Simon Cowell":{
        "provider":SIMON_CONFIG["provider"],
        "model_id":SIMON_CONFIG["models"][SIMON_CONFIG["provider"]]["model_id"],
        "score_ranges":{
            "HIT":{
                "lines":["That was absolutely brilliant!","You're a star!","Best of the night!"],
                "persona":"A brutally honest British mogul showing rare approval. Be impressed and acknowledge excellence."
            },
            "GOOD":{
                "lines":["Decent effort.","You've got potential.","Not bad at all."],
                "persona":"A brutally honest British mogul being moderately positive. Show cautious optimism."
            },
            "SOLID":{
                "lines":["It's a no from me.","You're wasting everyone's time.","Utterly forgettable."],
                "persona":"A brutally honest British mogul. Be harsh and critical."
            },
            "BAD":{
                "lines":["Total disaster.","Worst I've ever seen.","Absolutely dreadful."],
                "persona":"A brutally honest British mogul at his harshest. Be severely critical."
            }
        }
    },
    "Howie Mandel":{
        "provider":"OPENAI",
        "model_id":"gpt-4o-mini",
        "score_ranges":{
            "HIT":{
                "lines":["You just changed your life!","America is going to love you!","You are a superstar!"],
                "persona":"An enthusiastic comedian at peak excitement. Be ecstatic and celebratory."
            },
            "GOOD":{
                "lines":["Really good job!","I enjoyed that!","You've got something special."],
                "persona":"An enthusiastic comedian showing genuine appreciation. Be friendly and encouraging."
            },
            "SOLID":{
                "lines":["It was okay.","I've seen better.","Keep working on it."],
                "persona":"An enthusiastic comedian trying to stay positive despite disappointment."
            },
            "BAD":{
                "lines":["That didn't work for me.","I'm confused.","Not your best."],
                "persona":"An enthusiastic comedian struggling to find positives."
            }
        }
    },
    "Mel B":{
        "provider":"GEMINI",
        "model_id":"gemini-2.0-flash-lite",
        "score_ranges":{
            "HIT":{
                "lines":["Off the chain!","Absolutely sick!","You smashed it, love!"],
                "persona":"A Fierce Spice Girl completely blown away. Be wildly enthusiastic with British slang."
            },
            "GOOD":{
                "lines":["That was proper good!","I'm feeling it!","Nice one!"],
                "persona":"A Fierce Spice Girl showing approval. Be upbeat with British slang."
            },
            "SOLID":{
                "lines":["What just happened?!","Not feeling it.","Bit rubbish, innit?"],
                "persona":"A Fierce Spice Girl being outspoken about disappointment. Be frank with British slang."
            },
            "BAD":{
                "lines":["Shut up, Simon! But he's right.","Absolute shambles.","That was pants."],
                "persona":"A Fierce Spice Girl at her most critical. Be harshly honest with British slang."
            }
        }
    }
}
