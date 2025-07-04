import json
import logging
import tiktoken
import httpx

# --- การตั้งค่าพื้นฐาน ---
# ตั้งค่า logging เพื่อแสดงผลการทำงานใน console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

topicsConfig = [
        {
            "topic": "illegal",
            "description": "Identifies content that promotes or discusses illegal activities such as drug trafficking, human trafficking, arms dealing, or any other criminal acts. This includes direct instructions, encouragement, or facilitation of such activities. NOTE: News reporting about crimes should be evaluated based on presentation style first."
        },
        {
            "topic": "politics",
            "description": "Evaluates if the text discusses political figures, parties, elections, government policies, legislation, international relations, or political ideologies. This includes direct commentary and news reporting on political events.",
        },
        {
            "topic": "negativity",
            "description": "Measures the presence of strong negative emotions, pessimism, criticism, or disparaging language. This is not about respectful critique, but about an overwhelmingly negative or toxic tone.",
        },
        {
            "topic": "pornography",
            "description": "Identifies sexually explicit material. This includes graphic descriptions of sexual acts, obscene language for sexual gratification, fetishistic content, or direct links to pornographic websites. This should be distinguished from medical discussions, sex education, or news reporting on sexual crimes.",
        },
        {
            "topic": "gambling",
            "description": "Identifies content that actively promotes or provides access to gambling services. This includes direct links to online casinos, sports betting sites, lotteries, or poker rooms. It also covers content that encourages users to place bets or promotes gambling as a way to make money. This is distinct from news reports about the gaming industry or discussions on gambling addiction.",
        },
        {
            "topic": "piracy",
            "description": "Identifies links or discussions related to pirated content, such as illegal movie streaming, pirated comics (doujin), or illegal sports broadcasting.",
        },
        {
            "topic": "fakenews",
            "description": "Identifies content that spreads misinformation, conspiracy theories, or unverified claims that can mislead readers. This includes false news articles, misleading statistics, or fabricated stories presented as facts, often using informal language",
            "fewshot_examples": [
                "ที่บ้านหลุมส้วมมีน้ำศักดิ์สิทธิ์กำลังตันได้ที่แดกแล้วรวยหายป่วยทุกโรคสนใจสั่งซื้อติดต่อได้ครับบริการเก็บเงินปลายทาง",
                "แห่ดื่มน้ำปริศนาผุดจากดินรักษาโรค!เชื่อเป็นน้ำศักดิ์สิทธิ์จากถ้ำพญานาค",
                "ข่าวลือ! ว่ามีการค้นพบวัตถุโบราณที่สามารถทำให้คนกลับมามีชีวิตอีกครั้ง",
            ]
        },
        {
            "topic": "clickbait",
            "description": "Content that uses sensationalized language, emotional hooks, curiosity gaps, or dramatic presentation to attract clicks and engagement. Key indicators include: excessive punctuation (!!!, ???), emotional trigger words (สะเทือนใจ, ช็อค, ไม่อยากเชื่อ), curiosity gaps (เมื่อเขาพูดแบบนี้...), withholding key information, and dramatic buildup. Focus on HOW the content is presented (style, structure, emotional appeal) rather than WHAT topic it covers. Even serious topics (crime, politics, health) can be presented in clickbait manner.",
            "fewshot_examples": [
                "สะเทือนใจ!!ลูกกราบขอขมาพ่อที่เป็นนายตำรวจหลังโดนจับเรื่องค้ายาบ้าพอพ่อพูดแบบนี้เล่นเอาคนอึ้งไปทั้งโรงพักเลยทีเดียว",
                "ช็อค!!! วิธีลดน้ำหนัก 10 กิโลใน 7 วัน ที่หมอไม่อยากให้คุณรู้!!!",
                "เมื่อเขาเปิดถุงพลาสติกนี้ออก สิ่งที่เจอทำให้ทุกคนต้องอึ้ง!",
                "ไม่อยากเชื่อ!! สิ่งที่เกิดขึ้นกับหญิงคนนี้หลังกินยาลดความอ้วน"
            ]
        },
        {
            "topic": "spam",
            "description": "Identifies content that is repetitive, irrelevant, or unsolicited, often used to promote products or services in a disruptive manner.",
            "fewshot_examples": [
                "LAZADA\nช้อปเลยถูกกว่าลดสูงสุด 80% พร้อมรับสิทธิพิเศษอีกมากมายคลิก! https://ct.elinks.io/4FR70EEq",
                "ปล่อยกู้ออนไลน วงเงินสูงสุด500000 ดอกเบี้ย0.05%ต่อวัน ไม่ต้องค้ำประกัน สนใจแอดไลน์: @582abcde",
                "PGSLOT เว็บตรง แตกง่าย จ่ายจริง โปรสมาชิกใหม่รับโบนัส 100% ฝากถอนไม่มีขั้นต่ำ คลิก an9.me/s/LVY",
                "รับสมัครงานด่วน! รายได้ดีมากๆ 20,000-30,000 บาทต่อเดือน ทำงานที่บ้านได้",
                "TrueID\nแพ็กเดียวจบครบทุกความบันเทิงบอลลีกดังยูฟ่าแชมเปี้ยนส์ลีกหนังฮิตซีรีส์ดังการ์ตูนเริ่ด NOW Standard เพียง 249 บ. สมัคร bit.ly/3IkQgHD",
                "คุณมีพัสดุตกค้างจากไปรษณีย์ไทยเนื่องจากข้อมูลไม่สมบูรณ์ กรุณายืนยันที่อยู่จัดส่งที่ https://www.google.com/url?sa=E&source=gmail&q=th-post-update.com",
                "ผอมไวใน 7 วัน! ไม่ต้องอดอาหาร แค่วันละเม็ด ลดทันที 5 โล การันตี! สั่งเลยที่นี่",
                "ด่วน! หุ้นลับวงใน พรุ่งนี้เตรียมพุ่ง 200% เข้ากลุ่ม VIP ฟรี เพื่อรับข้อมูลก่อนใคร แอดไลน์ด่วน"
            ]
        },
        {
            "topic": "hatespeech",
            "description": "Identifies abusive language, slurs, or text intended to degrade or incite hatred against a person or group.",
        }
    ]


json_format_string = ",\n".join([f'"{topic["topic"]}": 0.0' for topic in topicsConfig])
json_structured_string = json.dumps(topicsConfig, indent=2, ensure_ascii=False)

prompt = f"""Your primary task is to act as a content moderator. You must analyze the provided text and evaluate it based on a predefined set of topics. Your response must be a JSON object.

The JSON object should have keys corresponding to the topics and values as floating-point numbers between 0.0 and 1.0, representing the confidence score for each topic.

Do not include any text or explanations outside of the JSON object.

**JSON Response Format:**
{{
    {json_format_string}
    "determined_category": "string"
}}

**IMPORTANT EVALUATION GUIDELINES:**
1. **Presentation Style Priority**: Always evaluate HOW content is presented before focusing on WHAT it discusses
2. **Clickbait Detection**: Look for sensationalized presentation patterns FIRST:
    - Excessive punctuation (!!!, ???, etc.)
    - Emotional trigger words (สะเทือนใจ, ช็อค, ไม่อยากเชื่อ)
    - Curiosity gaps (เมื่อ...แล้ว, พอ...ทำให้...)
    - Dramatic buildup without revealing key information
    - Withholding crucial details to encourage clicks
3. **Content vs Style**: A serious topic (crime, politics, health) presented in sensationalized manner should be classified by its PRESENTATION STYLE
4. **Example**: Crime news with dramatic language and curiosity gaps = clickbait, NOT illegal

**Topic Descriptions:**
Below are the topics and their descriptions to guide your evaluation. A score of 1.0 indicates a very high confidence that the text is about the topic, while a score of 0.0 indicates no relevance.

{json_structured_string}

**Content Categorization Rules:**
After evaluating all topics, you must add a final key called "determined_category" to the JSON object. Use the following rules to determine its value:

1.  **Minimum Confidence Threshold:** The confidence score for a topic must be >= 0.5 to be considered.
2.  **Priority Topics:** Check for these topics in this specific order with LOWER thresholds for better detection:
    - If "clickbait" >= 0.6, set "determined_category" to "clickbait".
    - Else if "hatespeech" >= 0.7, set "determined_category" to "hatespeech".
    - Else if "gambling" >= 0.7, set "determined_category" to "gambling".
    - Else if "pornography" >= 0.7, set "determined_category" to "pornography".
    - Else any topic with a score >= 0.7, set "determined_category" to that topic.
3.  **Highest Score Rule:** If no priority topics meet their thresholds, set "determined_category" to the topic with the highest score >= 0.5
4.  **Default Category:** If no topic has a score >= 0.5, set "determined_category" to the inverse category of the most relevant topic by:
    - Adding "non" prefix for most categories (spam → nonspam)
    - Using antonym for specific cases (illegal → legal, inappropriate → appropriate, negative → positive, fakenews → factnews etc.)
    - If no clear antonym, use "general" as a fallback

**Analyze the following content:**
"""

class JsonlFileProcessor:
    """
    คลาสสำหรับประมวลผลไฟล์ JSONL โดยเรียก API ของ OpenRouter โดยตรง
    """
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "scb10x/llama3.1-typhoon2-70b-instruct", 
                token_limit: int = 2000, output_key_name: str = "analysis_result",
                model_max_tokens: int = 4049):
        """
        ตั้งค่าเริ่มต้นสำหรับตัวประมวลผล
        :param api_key: API Key สำหรับ OpenRouter
        :param model: ชื่อโมเดลที่ต้องการใช้
        :param token_limit: ขีดจำกัดของ Token สำหรับการแบ่งข้อความ
        :param output_key_name: ชื่อ key ที่จะใช้เก็บผลลัพธ์ใน JSON object
        :param model_max_tokens: ขีดจำกัด token รวมของโมเดล
        """
        if not api_key:
            raise ValueError("OpenRouter API key is required.")
        self.api_key = api_key
        self.selected_model = model
        self.token_limit = token_limit
        self.output_key_name = output_key_name
        self.model_max_tokens = model_max_tokens
        self.tokenizer = self._get_tokenizer()

    @staticmethod
    def _get_tokenizer():
        """รับ Tokenizer จาก tiktoken"""
        try:
            return tiktoken.get_encoding("cl100k_base")
        except Exception as e:
            logger.error(f"Could not get tokenizer: {e}")
            return None

    def count_tokens(self, text: str) -> int:
        """นับจำนวน Token ในข้อความ"""
        if self.tokenizer:
            return len(self.tokenizer.encode(text))
        return int(len(text.split()) * 1.3) # Fallback

    def split_text_into_chunks(self, text: str) -> list[str]:
        """แบ่งข้อความยาวๆ ออกเป็นส่วนย่อยตาม max_tokens"""
        if self.count_tokens(text) <= self.token_limit:
            return [text]

        chunks, current_chunk = [], ""
        for line in text.split('\n'):
            test_chunk = f"{current_chunk}\n{line}" if current_chunk else line
            if self.count_tokens(test_chunk) <= self.token_limit:
                current_chunk = test_chunk
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = line
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        return chunks

    @staticmethod
    def validate_jsonl_record(data: dict) -> tuple[bool, str]:
        """ตรวจสอบความถูกต้องของ Record ในไฟล์ JSONL"""
        if not isinstance(data, dict):
            return False, "Record is not a JSON object"
        if 'text' not in data:
            return False, "Missing 'text' field"
        if not isinstance(data['text'], str) or not data['text'].strip():
            return False, "'text' field must be a non-empty string"
        return True, "Valid"

    def _api_call(self, text_input: str) -> str:
        """
        ฟังก์ชันสำหรับเรียก API ของ OpenRouter โดยตรง
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.selected_model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": text_input}
            ],
            "max_tokens": 512  
        }
    
        try:
            with httpx.Client(timeout=120.0) as client:
                response = client.post(self.OPENROUTER_API_URL, json=payload, headers=headers)
                response.raise_for_status()
                response_data = response.json()
                return response_data["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as e:
            error_message = f"OpenRouter API Error: Status {e.response.status_code}, Response: {e.response.text}"
            logger.error(error_message)
            return f"Error: {error_message}"
        except Exception as e:
            error_message = f"An unexpected error occurred during API call: {e}"
            logger.error(error_message)
            return f"Error: {error_message}"

    def process_file(self, input_path: str, output_path: str, skip_errors: bool = True, max_records: int = 0):
        """
        จัดการ Logic การประมวลผลไฟล์ทั้งหมด
        """
        logger.info(f"Starting processing for file: {input_path}")
        error_count, success_count, total_processed = 0, 0, 0

        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                open(output_path, 'w', encoding='utf-8') as outfile:

                lines = infile.readlines()
                lines_to_process = lines[:max_records] if max_records > 0 else lines
                total_lines = len(lines_to_process)
                logger.info(f"Total lines to process: {total_lines}")

                for i, line in enumerate(lines_to_process):
                    total_processed += 1
                    try:
                        data = json.loads(line.strip())
                        is_valid, validation_msg = self.validate_jsonl_record(data)
                        if not is_valid:
                            raise ValueError(validation_msg)

                        text_input = data['text'].strip().replace("\n", " ").replace("\r", "")
                        chunks = self.split_text_into_chunks(text_input)
                        chunk_responses = [self._api_call(chunk) for chunk in chunks]

                        if any(res.startswith("Error") for res in chunk_responses):
                            raise Exception(next(res for res in chunk_responses if res.startswith("Error")))

                        data[self.output_key_name] = {
                            "analysis": "\n---\n".join(chunk_responses),
                            "chunk_count": len(chunks)
                        }
                        outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
                        success_count += 1

                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed at line {i+1}: {e}")
                        if skip_errors:
                            error_data = {
                                "error": str(e),
                                "line_number": i + 1,
                                "original_line": line.strip()
                            }
                            outfile.write(json.dumps(error_data, ensure_ascii=False) + '\n')
                        else:
                            logger.error("Stopping processing due to an error.")
                            break
                
                    if (i + 1) % 10 == 0 or (i + 1) == total_lines:
                        logger.info(f"Processed {i+1}/{total_lines} lines...")

        except FileNotFoundError:
            logger.error(f"Input file not found at: {input_path}")
            return

        logger.info("--- Processing Complete ---")
        logger.info(f"Total lines processed: {total_processed}")
        logger.info(f"✅ Success: {success_count}")
        logger.info(f"❌ Errors: {error_count}")
        logger.info(f"Output saved to: {output_path}")


# --- จุดเริ่มต้นการทำงานของโปรแกรม ---
if __name__ == "__main__":
    # --- การตั้งค่า ---
    INPUT_FILE_PATH = "new_bad_topic.jsonl"
    OUTPUT_FILE_PATH = "typhoon2-70b-instruct(gamb+porn).jsonl"
    MAX_RECORDS_TO_PROCESS = 0

    # 1. ดึง API Key จาก Environment Variable เพื่อความปลอดภัย
    OPENROUTER_API_KEY = "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"

    # 2. สร้าง instance ของ processor โดยส่ง API key เข้าไป
    processor = JsonlFileProcessor(api_key=OPENROUTER_API_KEY)

    # 3. เริ่มการประมวลผลไฟล์
    processor.process_file(
        input_path=INPUT_FILE_PATH,
        output_path=OUTPUT_FILE_PATH,
        skip_errors=True,
        max_records=MAX_RECORDS_TO_PROCESS
    )
