# Import necessary libraries
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

if __name__ == '__main__':
    # Load the wine dataset
    wine = load_wine()
    X, y = wine.data, wine.target

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=7)

    # Train a Random Forest classifier
    model = RandomForestClassifier(n_estimators=100, random_state=7)
    model.fit(X_train, y_train)

    # Save the model to a file
    joblib.dump(model, 'wine_model.pkl')
    
    print("The model training was successful")
