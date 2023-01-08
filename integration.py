# Integration of entire pipeline
def integration(method):
    # Load libraries
    import data_simulation as ds
    import machine_learning as ml
    import azure_digital_twin as adt
    
    # Iterate through real-time data simulation
    for i in ds.simulate_real_time(0.1, 0.1, 0.1, 0.5):
        # Print data
        print(i[["Time", "Proximity", "Engine Speed", "Pressure"]].to_string(index = False))

         # Send data to Digital Twin
        if method == "send_to_twin":
            # Check data for anomalies
            if ml.detect_anomalies(i[["Proximity", "Engine Speed", "Pressure"]]) == "correct":
                # Update digital twin
                adt.set_proximity(float(i["Proximity"]))
                adt.set_engine_speed(int(i["Engine Speed"]))
                adt.set_pressure(int(i["Pressure"]))

                # Print success message
                print("Digital Twin was successfully updated.")
                
                # Check updated values
                adt.print_proximity()
                adt.print_engine_speed()
                adt.print_pressure()

                # Visualize state of the digital twin
                adt.plot_sensor_values()
            else:
                # Print failure message
                print("Digital Twin was not updated.")
                #clear_output(wait=True)

integration(method = "send_to_twin")