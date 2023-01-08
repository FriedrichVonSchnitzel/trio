# Function to train a random forest model
def train_model(data):
    # Import libraries
    import pandas as pd
    import seaborn as sns
    from sklearn.linear_model import LogisticRegression
    from sklearn import svm
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.neural_network import MLPClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, f1_score, recall_score, precision_score

    # Load data
    data = pd.read_csv(data)

    # Select attributes
    X = data.loc[:, ["Proximity", "Engine Speed", "Pressure"]]
    y = data["Label"]

    # Split data in training and test data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=99)
    test_data = pd.concat([pd.DataFrame(X_test), pd.DataFrame(y_test)], axis=1)

    # Train the Random Forest model
    rf_model = RandomForestClassifier(n_estimators=100)
    rf_model.fit(X_train, y_train)

    # Make predictions
    test_data["Random Forest"] = rf_model.predict(X_test)

    # Evaluation: create a confusion matrix
    y_prediction_rf = test_data["Random Forest"]
    y_actual = test_data["Label"]
    confusion_matrix_rf = pd.crosstab(y_prediction_rf, y_actual)
    # Outside of Jupyter, use print() instead of display()
    print(confusion_matrix_rf)

    # Evaluation: calculate performance metrics
    print(f'Accuracy: {round(accuracy_score(y_actual, y_prediction_rf) * 100, 2)}%')
    print(f'Precision: {round(precision_score(y_actual, y_prediction_rf, pos_label="anomaly") * 100, 2)}%')
    print(f'Recall: {round(recall_score(y_actual, y_prediction_rf, pos_label="anomaly") * 100, 2)}%')
    print(f'F1 Score: {round(f1_score(y_actual, y_prediction_rf, pos_label="anomaly") * 100, 2)}%')



# Function to apply the trained model and make predictions
def detect_anomalies(data_row):
    # Load libraries
    import skops.io as sio

    # Load the model from the file
    trained_model = sio.load(file = "data/trained_model.skops", trusted = True)

    # Make prediction and print result
    if(trained_model.predict(data_row) == "correct"):
        print("No anomaly was detected ✅")
        return "correct"
    else:
        print("An anomaly was detected ❌")
        return "anomaly"    