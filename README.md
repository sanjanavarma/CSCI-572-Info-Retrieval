# CSCI-572-Info-Retrieval
HW1: Comparing Search Engines
Objective
Compare the effectiveness and relevance of search results from Google and one alternative search engine (e.g., Bing, DuckDuckGo, Yahoo, Ask Jeeves, etc.).

Tasks
Formulate and submit 100 search queries.

Record and compare results from both Google and the chosen non-Google engine.

Analyze result relevance, ranking quality, and overlap.

Optionally explore newer engines like Perplexity.ai or OpenAI search (if available).

Skills Practiced
Search relevance evaluation

Ranking comparison

Qualitative and quantitative analysis of search engines

HW2: Web Crawling
Objective
Implement a simple web crawler using a third-party API such as crawler4j in Java.

Tasks
Start with a manually initialized queue of seed URLs.

Recursively crawl web pages:

Visit and fetch the content of each URL.

Extract and enqueue new links.

Continue until the URL queue is empty.

Save visited pages and manage URL depth and duplicates.

Skills Practiced
Web scraping basics

Recursive data structures (queues, graphs)

Using existing APIs for large-scale data collection

HW3: Inverted Index Creation
Objective
Build an inverted index by parsing multiple text files and mapping words to their occurrences.

Tasks
Read a collection of text files.

Tokenize and normalize words.

Construct an index with entries like:
word doc_id1:count1 doc_id2:count2 ...

Skills Practiced
File parsing and string processing

Dictionary/hash map manipulation

Fundamentals of search engine indexing

HW4: LLMs and Retrieval-Augmented Generation (RAG)
Objective
Explore and experiment with LLM-driven applications, including RAG systems.

Tools and Tasks
Use Ollama to run local LLMs on your machine.

Create RAG pipelines using LangFlow by DataStax.

Use Claude Desktop or MCP to apply RAG on local documents.

Skills Practiced
LLM invocation and configuration

RAG pipeline design and execution

Visual programming with LangFlow

Local file ingestion and semantic retrieval
