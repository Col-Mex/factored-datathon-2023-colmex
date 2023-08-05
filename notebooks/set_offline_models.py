from transformers import AutoTokenizer, BertTokenizer, BertModel

tokenizer_roberta = BertTokenizer.from_pretrained("notebooks/models/roberta")
model_roberta = BertModel.from_pretrained("notebooks/models/roberta")
tokenizer_roberta.save_pretrained("notebooks/models/offline_mode/roberta_offline")
model_roberta.save_pretrained("notebooks/models/offline_mode/roberta_offline")

tokenizer_emotions = AutoTokenizer.from_pretrained("notebooks/models/emotions")
model_emotions = BartConfig.from_pretrained("notebooks/models/emotions")
tokenizer_emotions.save_pretrained("notebooks/models/offline_mode/emotions_offline")
model_emotions.save_pretrained("notebooks/models/offline_mode/emotions_offline")
