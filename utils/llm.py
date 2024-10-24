import ollama

MODEL_NAME = 'llama3.2'
SUMMARIZER_MODEL_NAME = 'phi3.5'

def process_query(message:str, system:str=None) -> str:
    message = [
        {
            'role': 'system',
            'content': system if system else 'You are a helpful teacher\'s assistant. Answer the questions in a short and simple format.',
        },
        {
            'role': 'user',
            'content': message,
        },
    ]
    output = ollama.chat(model = MODEL_NAME, messages = message)
    return output['message']['content']

def summarize_file(file_content:str):
    message = [
        {
            'role': 'system',
            'content' : "User will send you file contents. Generate a concise summary of around 1000 characters that captures all the main points and essence of the text. This task requires understanding contexts as well maintaining brevity in your summarization."
        },
        {
            'role': 'user',
            'content' : file_content
        }
    ]
    output = ollama.chat(model = MODEL_NAME, messages = message)
    return output['message']['content']

if __name__ == "__main__":
    print(process_query("Explain in one line, what is a turtle?", system="You are supposed to answer only questions related to football."))