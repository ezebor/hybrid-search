import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.neighbors import NearestNeighbors

class ProductRecommender:
    """
    A KNN-based recommender system using TF-IDF for text and StandardScaler for price.
    This class is designed to be trained, serialized, and then used for predictions.
    """

    def __init__(self, k=5):
        self.k = k
        self.pipeline = None
        self.model = None
        self.product_ids = None

        # Define the preprocessing pipeline for consistent transformation
        preprocessor = ColumnTransformer(
            transformers=[
                ('text', TfidfVectorizer(stop_words='english', max_features=5000), 'full_text'),
                ('numeric', StandardScaler(), ['price'])
            ],
            remainder='drop'
        )
        self.pipeline = Pipeline(steps=[('preprocessor', preprocessor)])

    def fit(self, df: pd.DataFrame):
        """
        Fits the preprocessing pipeline and the NearestNeighbors model on the data.
        """
        df['full_text'] = df['name'].fillna('') + ' ' + df['description'].fillna('')

        X_transformed = self.pipeline.fit_transform(df)

        self.model = NearestNeighbors(n_neighbors=self.k, metric='cosine')
        self.model.fit(X_transformed)

        self.product_ids = df['id'].values
        print("Recommender model has been successfully trained.")

    def recommend(self, name: str, description: str, price: float) -> list[int]:
        """
        Transforms a query and finds the K most similar products.
        """
        if self.pipeline is None or self.model is None:
            raise RuntimeError("The recommender has not been fitted yet. Call .fit() first.")

        query_df = pd.DataFrame([{'name': name, 'description': description, 'price': price}])
        query_df['full_text'] = query_df['name'] + ' ' + query_df['description']

        query_transformed = self.pipeline.transform(query_df)

        distances, indices = self.model.kneighbors(query_transformed)

        neighbor_indices = indices[0]
        recommended_ids = self.product_ids[neighbor_indices]

        return recommended_ids.tolist()