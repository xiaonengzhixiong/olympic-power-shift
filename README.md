本项目的 AI 功能需要调用大模型，因此无法在静态网页中直接使用。
若需要完整体验 AI 功能，请按以下步骤在本地运行：
1.下载项目全部文件到本地
2.在项目根目录创建 .env 文件，内容为：
"DEEPSEEK_API_KEY=sk-******"
3.安装依赖：
pip install flask requests python-dotenv
4.启动后端服务：
python server.py
浏览器访问 http://localhost:8000
除AI功能外，其他所有可视化页面均可在 GitHub Pages 在线版本中正常使用。
