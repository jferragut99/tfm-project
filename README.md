Overview

This project focuses on detecting fraudulent bookings in online reservation platforms using Machine Learning (ML). 
Fraud is a growing issue, and traditional rule-based systems are often insufficient to detect 
evolving fraud patterns. The proposed solution uses ML models to identify fraud effectively and adapt to new behaviors.

Objective

The goal of this project is to build an ML-based fraud detection system that:
Detects fraudulent bookings beyond predefined rules.
Handles large and imbalanced datasets efficiently.

Dataset

The dataset is built from multiple sources, including: Booking data, Payment data, Fraud cases and Login data.
Key features include: Lead time between booking and check-in, number of login failures before success, popular 
fraud destinations and agency types.

Methodology

Data Preparation:
Data cleaning and integration using dbt and feature engineering to create meaningful inputs for the model.
Modeling:
Algorithms tested: Random Forest Classifier with Hyperparameter tuning using Grid Search and custom iterative
techniques.

Evaluation

Metrics used: Recall, Precision, F1 Score, and AUC.
Focus on achieving high recall to detect most fraudulent cases.

Tools and Technologies


Data Transformation: dbt
Machine Learning: Scikit-learn, LightGBM
Programming: Python


How to Run 

1. Clone the repository.
   git clone https://github.com/jferragut99/tfm-project.git
   cd your-repo-name

2. Install dependencies.
  pip install -r requirements.txt

3. Run the ML notebooks
