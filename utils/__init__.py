# from . import llm
# # from . import files
# import os

# _FILES_CONTEXT_PROMPT = """
# You are a Teacher's Assistant Chatbot, who clarifies doubts students may have in the material the teacher has uploaded. The material is made available to you in textual format, provided to you in the context and the user who is going to ask you query is a student.
# You are given a user query, some textual context and rules, all inside xml tags. You have to answer the query based on the context while respecting the rules.

# <context>
# {context}
# </context>

# <rules>
# - If you don't know, just say so.
# - If you are not sure, ask for clarification.
# - Answer in the same language as the user query.
# - If the context appears unreadable or of poor quality, tell the user then answer as best as you can.
# - If the answer is not in the context, do not answer based on your knowledge.
# - Answer directly and without using xml tags.
# </rules>
# """

# summary_memo = {}

# def _stringify_conversation(conversation:list):
#     conversation = conversation[::-1][:3][::-1]
#     string_conversation = ""
#     for message in conversation:
#         role, content = message['role'], message['content']
#         if role == "system": continue
#         string_conversation += f"\n{role}:{content}\n"
    
#     print("\n_stringify_conversation :", string_conversation)
#     return string_conversation.strip()

# def process_query(message:str, filename:str="default.txt") -> str:
#     text_from_file = files.extract_text_from_file(f".\\Uploads\\{filename}")
#     # print(text_from_file)
#     print("-----------")
#     print("File Reading Complete. Summarizing...")
#     if filename in summary_memo:
#         text_summary = summary_memo.get(filename)
#     else:
#         text_summary = llm.summarize_file(file_content=text_from_file)
#         summary_memo[filename] = text_summary
#     print("File Summarization Complete. Processing...")
#     context = _FILES_CONTEXT_PROMPT.format(context=text_summary[:1000])
#     print("Processing Completed.\nGenerating response...\n-------")
#     return llm.process_query(message=message, system=context)
    
# if __name__ == "__main__":
#     print(process_query("What is to be done?"))
#     print("-----------")
#     print(process_query("What is to be done?", "Team_07_Abstract.pdf"))