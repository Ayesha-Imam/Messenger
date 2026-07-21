# Messenger

## 🚀 First-Time Installation

### Prerequisites

- Install **Python 3.13.14** (required)

---

## 📥 Clone the Project

Clone the repository:

```bash
git clone https://github.com/Ayesha-Imam/Messenger.git
```

Navigate into the project directory:

```bash
cd Messenger
```

---

## 🐍 Create and Activate Virtual Environment

Create a virtual environment:

```bash
py -3.13 -m venv .venv
```

Activate it (Windows):

```bash
.venv\Scripts\activate.bat
```

Install all required dependencies:

```bash
python -m pip install -r requirements.txt
```

> **Note:** Installing dependencies may take a few minutes.

---

## ⚙️ Environment Variables

Create a `.env` file in the project's root directory.

> **Note:** You'll need to ask me for the `.env` file. 😄

---

## ▶️ Run the Project

Start the development server:

```bash
python -m uvicorn main:app --reload --port 9077
```

The application will be available at:

```
http://localhost:9077
```

---

# 🏃 Running the Project (Every Time)

Whenever you want to run the project again:

### 1. Activate the virtual environment

```bash
.venv\Scripts\activate.bat
```

### 2. Start the server

```bash
uvicorn main:app --reload --port 9077
```

---

# 📦 Updating Dependencies

If you install any new Python packages, regenerate the `requirements.txt` file:

```bash
pipreqs . --force --encoding utf-8
```

---

## 📁 Project Structure

```
Messenger/
│── .venv/
│── .env
│── requirements.txt
│── main.py
└── ...
```
