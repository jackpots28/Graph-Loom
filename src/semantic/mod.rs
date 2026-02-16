//! This module serves as the main interface for various functionalities provided in the library.
//! It contains sub-modules that focus on specific tasks related to large language models, data extraction,
//! retrieval-augmented generation, and embeddings.
//!
//! # Available Modules
//!
//! - `llm_client`: This module provides functionality to interact with large language models (LLMs)
//!                  such as making requests, handling responses, and managing interactions with the models.
//! - `extraction`: A module responsible for extracting relevant information from data sources.
//!                  It can include text parsing, data cleaning, and other information extraction utilities.
//! - `rag`: The "Retrieval-Augmented Generation (RAG)" module is designed to assist with fetching
//!           relevant documents or context and using them to generate improved responses from a language model.
//! - `embeddings`: This module implements functionality for handling embeddings, including generating embedding
//!                  vectors, comparing embeddings, and other tasks related to semantic representations.
pub mod llm_client;
pub mod extraction;
pub mod rag;
pub mod embeddings;
