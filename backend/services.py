import re
import time
import queue
import threading
import requests
import streamlit as st
from openai import OpenAI
import google.generativeai as genai
from config import OPENAI_API_KEY,FRIENDLI_API_KEY,GITHUB_API_KEY,GOOGLE_API_KEY,LASTFM_API_KEY,SIMON_CONFIG,JUDGES


@st.cache_resource
def init_api_clients():
    clients={}
    try:
        if OPENAI_API_KEY:
            clients["openai"]=OpenAI(api_key=OPENAI_API_KEY)
        if FRIENDLI_API_KEY:
            clients["friendli"]=OpenAI(api_key=FRIENDLI_API_KEY,base_url=SIMON_CONFIG["models"]["FRIENDLI"]["base_url"])
        if GITHUB_API_KEY:
            clients["github"]=OpenAI(api_key=GITHUB_API_KEY,base_url=SIMON_CONFIG["models"]["GITHUB_LLAMA"]["base_url"])
    except Exception as e:
        st.warning(f"클라이언트 초기화 경고: {e}")
    try:
        if GOOGLE_API_KEY:
            genai.configure(api_key=GOOGLE_API_KEY)
            clients["gemini"]=genai
    except Exception as e:
        st.warning(f"Gemini 초기화 경고: {e}")
    return clients


API_CLIENTS=init_api_clients()


def parse_ai_response(text):
    if not text or len(text.strip()) < 10:
        return {"Musicality":25,"Marketability":25,"Narrative":23,"Total":73},"상세 피드백이 없습니다."
    
    clean_text=text.strip()
    scores={"Musicality":0,"Marketability":0,"Narrative":0,"Total":0}
    
    patterns={
        "Musicality":[
            r"Musicality\s*[:：-]\s*(\d+)",
            r"Musicality\s*[=]\s*(\d+)",
            r"\*\*Musicality\*\*\s*[:：-]\s*(\d+)"
        ],
        "Marketability":[
            r"Marketability\s*[:：-]\s*(\d+)",
            r"Marketability\s*[=]\s*(\d+)",
            r"\*\*Marketability\*\*\s*[:：-]\s*(\d+)"
        ],
        "Narrative":[
            r"Narrative\s*[:：-]\s*(\d+)",
            r"Narrative\s*[=]\s*(\d+)",
            r"\*\*Narrative\*\*\s*[:：-]\s*(\d+)"
        ],
        "Total":[
            r"Total\s*[:：-]\s*(\d+)",
            r"Total\s*[=]\s*(\d+)",
            r"\*\*Total\*\*\s*[:：-]\s*(\d+)"
        ]
    }
    
    for category,pattern_list in patterns.items():
        for p in pattern_list:
            m=re.search(p,clean_text,re.I|re.M)
            if m:
                v=int(m.group(1))
                scores[category]=min(v,100 if category=="Total" else 40)
                break
    
    if scores["Total"]==0:
        scores["Total"]=min(scores["Musicality"]+scores["Marketability"]+scores["Narrative"],100)
    
    if scores["Total"]==0:
        scores={"Musicality":25,"Marketability":25,"Narrative":23,"Total":73}
    
    comment_patterns=[
        r"Comment\s*[:：-]\s*(.*)",
        r"\*\*Comment\*\*\s*[:：-]\s*(.*)",
        r"Feedback\s*[:：-]\s*(.*)"
    ]
    
    comment="상세 피드백이 없습니다."
    for cp in comment_patterns:
        comment_match=re.search(cp,clean_text,re.S|re.I)
        if comment_match:
            comment=comment_match.group(1).strip()
            break
    
    if comment=="상세 피드백이 없습니다." and len(clean_text)>50:
        lines=clean_text.split('\n')
        comment_lines=[l for l in lines if not any(k in l for k in ["Musicality","Marketability","Narrative","Total"])]
        if comment_lines:
            comment=' '.join(comment_lines).strip()
    
    return scores,comment[:700]


def determine_grade_range(total_score):
    if total_score>=245:
        return "대박"
    if total_score>=190:
        return "좋음"
    if total_score>=150:
        return "무난"
    return "별로"


def get_system_prompt(judge_name,judge_info,grade):
    range_config=judge_info["score_ranges"][grade]
    scoring="엄격한 점수 규칙: Musicality 15-30, Marketability 15-30, Narrative 15-28, Total 45-78. 절대 이 범위를 초과하지 마세요."
    persona_guidelines={
        "Simon Cowell":"거친 영국식 유머로 4-6문장을 자세히 작성하세요. 냉정하게 분석하세요.",
        "Howie Mandel":"열정적인 4-6문장을 작성하세요. 에너지 넘치고 격려하되 솔직해야 합니다.",
        "Mel B":"영국식 슬랭을 섞어 5-7문장을 강하게 작성하세요. 직설적이고 열정적으로 개성을 보여주세요."
    }
    format_instruction="반드시 아래 형식 그대로 답변하세요: Musicality: [number]/40\nMarketability: [number]/40\nNarrative: [number]/40\nTotal: [sum]\nComment: [4-7문장 상세 피드백]"
    return f"당신은 {judge_name}입니다. {range_config['persona']} 유행어: {', '.join(range_config['lines'])}. {scoring} {persona_guidelines.get(judge_name,'')} 오직 한글로만 답변하세요. {format_instruction}"


def stream_judge_task(judge_name, judge_info, song_context, tags, grade, q):
    system_prompt = get_system_prompt(judge_name, judge_info, grade)
    tags_text = f"태그: {', '.join(tags)}" if tags else "태그: 사용 불가"
    user_prompt = f"평가 대상: {song_context}\n{tags_text}"
    full_text = ""
    start_time = time.time()
    q.put((judge_name, "", False, "로딩", judge_info["provider"]))
    
    try:
        provider = judge_info["provider"]
        
        if provider in ["GITHUB_LLAMA", "FRIENDLI"]:
            client = API_CLIENTS.get("friendli" if provider == "FRIENDLI" else "github")
            if not client:
                raise Exception(f"{provider}용 클라이언트가 초기화되지 않았습니다")
            
            stream = client.chat.completions.create(
                model=judge_info["model_id"],
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=700,
                stream=True
            )
            
            for chunk in stream:
                if hasattr(chunk.choices[0].delta, 'content') and chunk.choices[0].delta.content:
                    txt = chunk.choices[0].delta.content
                    full_text += txt
                    q.put((judge_name, txt, False, "생성 중", provider))
                    
        elif provider == "OPENAI":
            client = API_CLIENTS.get("openai")
            if not client:
                raise Exception("OpenAI 클라이언트가 초기화되지 않았습니다")
            
            stream = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.7,
                max_tokens=700,
                stream=True
            )
            
            for chunk in stream:
                if chunk.choices and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta
                    if hasattr(delta, 'content') and delta.content:
                        txt = delta.content
                        full_text += txt
                        q.put((judge_name, txt, False, "생성 중", provider))
                    
        elif provider == "GEMINI":
            model = API_CLIENTS.get("gemini").GenerativeModel(judge_info["model_id"])
            response = model.generate_content(
                f"{system_prompt}\n\n{user_prompt}",
                generation_config={"temperature": 0.7, "max_output_tokens": 700},
                stream=True
            )
            
            for chunk in response:
                if hasattr(chunk, 'text') and chunk.text:
                    full_text += chunk.text
                    q.put((judge_name, chunk.text, False, "생성 중", provider))
        
        if not full_text or len(full_text.strip()) < 10:
            raise Exception("생성된 내용이 없거나 응답이 너무 짧습니다")
            
        q.put((judge_name, full_text, True, "완료", provider, round(time.time() - start_time, 2)))
        
    except Exception as e:
        error_msg = f"오류: {str(e)}"
        st.error(f"{judge_name} 평가 실패: {error_msg}")
        fallback_response=f"Musicality: 25/40\nMarketability: 25/40\nNarrative: 23/40\nTotal: 73\nComment: 기술적 문제로 평가를 완료할 수 없었습니다."
        q.put((judge_name, fallback_response, True, "오류", judge_info.get("provider", "UNKNOWN"), round(time.time() - start_time, 2)))


@st.cache_data(ttl=600)
def get_lastfm_data(artist, title):
    try:
        artist_encoded = requests.utils.quote(artist)
        title_encoded = requests.utils.quote(title)
        url = f"http://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={LASTFM_API_KEY}&artist={artist_encoded}&track={title_encoded}&format=json"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            st.warning(f"Last.fm API 오류 ({artist} - {title}): {data.get('message', '알 수 없는 오류')}")
            return None, []
        
        track = data.get("track", {})
        tags = [t["name"] for t in track.get("toptags", {}).get("tag", [])[:5]]
        
        album = track.get("album", {})
        images = album.get("image", [])
        img = None
        if images:
            for size in ["extralarge", "large", "medium"]:
                for image in images:
                    if image.get("size") == size and image.get("#text"):
                        img = image["#text"]
                        break
                if img:
                    break
        
        if not img and images:
            img = images[-1].get("#text")
        
        return img, tags
        
    except requests.exceptions.RequestException as e:
        st.warning(f"Last.fm API 요청 실패 ({artist} - {title}): {str(e)}")
        return None, []
    except Exception as e:
        st.warning(f"Last.fm API 예기치 못한 오류 ({artist} - {title}): {str(e)}")
        return None, []

def run_judge_panel(song,tags,img_col,s_col,h_col,m_col):
    areas={"Simon Cowell":s_col.empty(),"Howie Mandel":h_col.empty(),"Mel B":m_col.empty()}
    judge_outputs={name:"" for name in JUDGES.keys()}
    judge_status={name:{"provider":"","state":"대기 중","elapsed":0} for name in JUDGES.keys()}
    q=queue.Queue()
    final_results={}
    song_ctx=f"Artist: {song['artist']}, Title: {song['title']}\nLyrics: {song['review']}"

    for jn,ji in JUDGES.items():
        threading.Thread(target=stream_judge_task,args=(jn,ji,song_ctx,tags,"GOOD",q),daemon=True).start()

    finished=0
    while finished<3:
        res_tuple=q.get()
        name,chunk_txt,is_done,state,provider=res_tuple[:5]
        judge_status[name].update({"provider":provider,"state":state})
        if is_done:
            finished+=1
            judge_status[name]["elapsed"]=res_tuple[5]
            scores,comment=parse_ai_response(chunk_txt)
            final_results[name]={"scores":scores,"comment":comment,"elapsed":res_tuple[5]}
        else:
            judge_outputs[name]+=chunk_txt

        for jn in JUDGES.keys():
            s=judge_status[jn]
            if s["state"]=="완료":
                result=final_results[jn]
                msg=f"**{jn}** 완료 {s['provider']} [{s['elapsed']:.1f}s] [총점: {result['scores']['Total']}]\n\n"
                msg+=f"**Musicality:** {result['scores']['Musicality']}/40\n\n"
                msg+=f"**Marketability:** {result['scores']['Marketability']}/40\n\n"
                msg+=f"**Narrative:** {result['scores']['Narrative']}/40\n\n"
                msg+=f"**Total:** {result['scores']['Total']}\n\n"
                msg+=f"**Comment:** {result['comment']}"
                areas[jn].info(msg)
            else:
                msg=f"**{jn}** {s['state']} {s['provider']}\n\n{judge_outputs[jn]}"
                areas[jn].info(msg)

    return final_results
