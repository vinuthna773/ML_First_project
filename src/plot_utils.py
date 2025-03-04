from datetime import timedelta
from typing import Optional

import pandas as pd
import plotly.express as px


def plot_aggregated_time_series(
    features: pd.DataFrame,
    targets: pd.Series,
    row_id: int,
    predictions: Optional[pd.Series] = None,
):
    """
    Plots the time series data for a specific location from NYC taxi data.

    Args:
        features (pd.DataFrame): DataFrame containing feature data, including historical ride counts and metadata.
        targets (pd.Series): Series containing the target values (e.g., actual ride counts).
        row_id (int): Index of the row to plot.
        predictions (Optional[pd.Series]): Series containing predicted values (optional).

    Returns:
        plotly.graph_objects.Figure: A Plotly figure object showing the time series plot.
    """
    # Extract the specific location's features
    location_features = features[features["pickup_location_id"] == row_id]
    
    if location_features.empty:
        print(f"No data found for row_id {row_id}")
        return None
        
    # Extract the corresponding target value
    actual_target = targets.loc[location_features.index]
    
    # Identify time series columns (historical ride counts)
    time_series_columns = [
        col for col in features.columns if col.startswith("rides_t-")
    ]
    
    # Extract time series values
    time_series_values = [location_features[col].iloc[0] for col in time_series_columns] + [actual_target.iloc[0]]
    
    # Get pickup hour timestamp
    pickup_hour = location_features["pickup_hour"].iloc[0]
    
    # Generate corresponding timestamps for the time series
    time_series_dates = pd.date_range(
        start=pickup_hour - timedelta(hours=len(time_series_columns)),
        end=pickup_hour,
        freq="h",
    )
    
    # Create the plot title with relevant metadata
    title = f"Pickup Hour: {pickup_hour}, Location ID: {row_id}"
    
    # Create the base line plot
    fig = px.line(
        x=time_series_dates,
        y=time_series_values,
        template="plotly_white",
        markers=True,
        title=title,
        labels={"x": "Time", "y": "Ride Counts"},
    )
    
    # Add the actual target value as a green marker
    fig.add_scatter(
        x=[time_series_dates[-1]],  # Last timestamp
        y=[actual_target.iloc[0]],  # Actual target value
        line_color="green",
        mode="markers",
        marker_size=10,
        name="Actual Value",
    )
    
    # Optionally add the prediction as a red marker
    if predictions is not None:
        if isinstance(predictions, pd.DataFrame) and "pickup_location_id" in predictions.columns:
            # If predictions is a DataFrame with pickup_location_id column
            pred_values = predictions[predictions["pickup_location_id"] == row_id]
            if not pred_values.empty:
                # Get the prediction value (handle both Series and value columns)
                if "predicted_demand" in pred_values.columns:
                    pred_value = pred_values["predicted_demand"].iloc[0]
                else:
                    # Assume the prediction value is in the first column
                    pred_value = pred_values.iloc[0, 0]
                    
                fig.add_scatter(
                    x=[time_series_dates[-1]],  # Last timestamp
                    y=[pred_value],  # Predicted value
                    line_color="red",
                    mode="markers",
                    marker_symbol="x",
                    marker_size=15,
                    name="Prediction",
                )
        else:
            # Assume predictions is a Series aligned with the features index
            try:
                pred_value = predictions.loc[location_features.index].iloc[0]
                fig.add_scatter(
                    x=[time_series_dates[-1]],  # Last timestamp
                    y=[pred_value],  # Predicted value
                    line_color="red",
                    mode="markers",
                    marker_symbol="x",
                    marker_size=15,
                    name="Prediction",
                )
            except (KeyError, IndexError):
                print("Could not plot prediction - index alignment issue")
    
    return fig


def plot_prediction(features: pd.DataFrame, prediction: pd.DataFrame):
    """
    Plots historical ride data along with a prediction point.
    
    Args:
        features (pd.DataFrame): DataFrame containing feature data, including historical ride counts.
        prediction (pd.DataFrame): DataFrame containing prediction data.
        
    Returns:
        plotly.graph_objects.Figure: A Plotly figure object showing the time series plot with prediction.
    """
    if features.empty:
        print("No feature data provided")
        return None
        
    # Identify time series columns (historical ride counts)
    time_series_columns = [
        col for col in features.columns if col.startswith("rides_t-")
    ]
    
    # Extract values 
    time_series_values = [features[col].iloc[0] for col in time_series_columns]
    
    if "predicted_demand" in prediction.columns:
        time_series_values += prediction["predicted_demand"].to_list()
    else:
        print("No 'predicted_demand' column found in prediction data")
        return None
    
    # Convert pickup_hour to timestamp
    pickup_hour = pd.Timestamp(features["pickup_hour"].iloc[0])
    
    # Generate corresponding timestamps for the time series
    time_series_dates = pd.date_range(
        start=pickup_hour - timedelta(hours=len(time_series_columns)),
        end=pickup_hour,
        freq="h",
    )
    
    # Create a DataFrame for plotting
    historical_df = pd.DataFrame(
        {"datetime": time_series_dates, "rides": time_series_values}
    )
    
    # Create the plot title with relevant metadata
    title = f"Pickup Hour: {pickup_hour}, Location ID: {features['pickup_location_id'].iloc[0]}"
    
    # Create the base line plot
    fig = px.line(
        historical_df,
        x="datetime",
        y="rides",
        template="plotly_white",
        markers=True,
        title=title,
        labels={"datetime": "Time", "rides": "Ride Counts"},
    )
    
    # Add prediction point
    fig.add_scatter(
        x=[pickup_hour],  # Last timestamp
        y=prediction["predicted_demand"].to_list(),
        line_color="red",
        mode="markers",
        marker_symbol="x",
        marker_size=10,
        name="Prediction",
    )
    
    return fig