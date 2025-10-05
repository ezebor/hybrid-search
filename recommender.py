import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.neighbors import NearestNeighbors


class ProductRecommender:
    """
    A KNN-based recommender system that only trains on and recommends active products.
    """

    def __init__(self, k=5):
        self.k = k
        self.pipeline = None
        self.model = None
        self.product_ids = None

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
        Filters for active products and then fits the model.
        """
        # --- MODIFICATION: Ensure 'active' column exists and filter the DataFrame ---
        if 'active' not in df.columns:
            # If the active column is missing, assume all products are active
            df['active'] = 1

        # Convert to boolean and filter for only active products
        df['active'] = df['active'].astype(bool)
        active_df = df[df['active'] == True].copy()

        if active_df.empty:
            print("No active products found to train the model.")
            self.model = None  # Ensure the model is cleared if no active products exist
            return

        active_df['full_text'] = active_df['name'].fillna('') + ' ' + active_df['description'].fillna('')

        X_transformed = self.pipeline.fit_transform(active_df)

        self.model = NearestNeighbors(n_neighbors=self.k, metric='cosine')
        self.model.fit(X_transformed)

        self.product_ids = active_df['id'].values
        print(f"Recommender model has been successfully trained on {len(active_df)} active products.")

    def recommend(self, name: str, description: str, price: float) -> list[int]:
        """
        Transforms a query and finds the K most similar products from the active set.
        """
        if self.pipeline is None or self.model is None:
            # This will be the case if no active products were available during training
            return []

        query_df = pd.DataFrame([{'name': name, 'description': description, 'price': price}])
        query_df['full_text'] = query_df['name'] + ' ' + query_df['description']

        query_transformed = self.pipeline.transform(query_df)

        distances, indices = self.model.kneighbors(query_transformed)

        neighbor_indices = indices[0]
        recommended_ids = self.product_ids[neighbor_indices]

        return recommended_ids.tolist()

