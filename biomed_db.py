import sqlite3

def connect_db():
    """Connect to or create the database."""
    conn = sqlite3.connect('biomed_study.db')
    cur = conn.cursor()
    return conn, cur

def create_tables(cur):
    """Create the three tables with exact schema."""
    # Patients table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Patients (
            patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            age INTEGER CHECK(age >= 18 AND age <= 90),
            gender TEXT CHECK(gender IN ('Male', 'Female', 'Other')),
            enrollment_date TEXT NOT NULL
        )
    ''')
    
    # Clinical Visits table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Clinical_Visits (
            visit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            visit_date TEXT NOT NULL,
            systolic_bp INTEGER,
            diastolic_bp INTEGER,
            blood_glucose_mmol_L REAL,
            notes TEXT,
            FOREIGN KEY (patient_id) REFERENCES Patients(patient_id) ON DELETE CASCADE
        )
    ''')
    
    # Samples table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Samples (
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            collection_date TEXT NOT NULL,
            sample_type TEXT CHECK(sample_type IN ('Blood', 'Serum', 'Plasma', 'Urine')),
            storage_location TEXT,
            FOREIGN KEY (patient_id) REFERENCES Patients(patient_id) ON DELETE CASCADE
        )
    ''')

def insert_data(conn, cur):
    """Insert sample data: 3 patients, 4 visits, 5 samples."""
    # Insert patients (using parameterized queries)
    patients = [
        ('John Doe', 45, 'Male', '2026-01-01'),
        ('Jane Smith', 52, 'Female', '2026-01-15'),
        ('Alex Johnson', 38, 'Other', '2026-02-01')
    ]
    cur.executemany('''
        INSERT INTO Patients (full_name, age, gender, enrollment_date)
        VALUES (?, ?, ?, ?)
    ''', patients)
    
    # Get patient IDs (for foreign keys)
    cur.execute('SELECT patient_id FROM Patients')
    patient_ids = [row[0] for row in cur.fetchall()]  # [1, 2, 3]
    
    # Insert clinical visits (distributed: Patient1:2, Patient2:1, Patient3:1)
    visits = [
        (patient_ids[0], '2026-01-10', 130, 85, 5.5, 'Stable'),
        (patient_ids[0], '2026-02-10', 145, 90, 6.0, 'High BP noted'),
        (patient_ids[1], '2026-01-20', 120, 80, 4.8, 'Good'),
        (patient_ids[2], '2026-02-05', 150, 95, 7.2, 'Monitor glucose')
    ]
    cur.executemany('''
        INSERT INTO Clinical_Visits (patient_id, visit_date, systolic_bp, diastolic_bp, blood_glucose_mmol_L, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', visits)
    
    # Insert samples (distributed: Patient1:2, Patient2:2, Patient3:1)
    samples = [
        (patient_ids[0], '2026-01-10', 'Blood', 'Fridge A-03'),
        (patient_ids[0], '2026-02-10', 'Serum', 'Biobank Rack 5'),
        (patient_ids[1], '2026-01-20', 'Plasma', 'Fridge A-03'),
        (patient_ids[1], '2026-01-25', 'Urine', 'Biobank Rack 5'),
        (patient_ids[2], '2026-02-05', 'Blood', 'Fridge A-03')
    ]
    cur.executemany('''
        INSERT INTO Samples (patient_id, collection_date, sample_type, storage_location)
        VALUES (?, ?, ?, ?)
    ''', samples)
    
    conn.commit()
    print("Data inserted successfully.")

def list_patients(cur):
    """Read: List all patients with age and enrollment date (simple SELECT)."""
    cur.execute('''
        SELECT full_name, age, enrollment_date FROM Patients
    ''')
    results = cur.fetchall()
    print("\nPatients List:")
    print("Name\t\tAge\tEnrollment Date")
    for row in results:
        print(f"{row[0]}\t{row[1]}\t{row[2]}")

def visits_for_patient(cur, patient_id=1):
    """Read: Show all visits for a specific patient (using JOIN)."""
    cur.execute('''
        SELECT p.full_name, v.visit_date, v.systolic_bp, v.diastolic_bp, v.blood_glucose_mmol_L, v.notes
        FROM Clinical_Visits v
        JOIN Patients p ON v.patient_id = p.patient_id
        WHERE v.patient_id = ?
    ''', (patient_id,))
    results = cur.fetchall()
    print(f"\nVisits for Patient ID {patient_id}:")
    print("Name\t\tVisit Date\tSys BP\tDia BP\tGlucose\tNotes")
    for row in results:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}")

def high_bp_patients(cur):
    """Read: Find patients with systolic BP > 140 in any visit (using subquery)."""
    cur.execute('''
        SELECT DISTINCT p.full_name, p.patient_id
        FROM Patients p
        WHERE p.patient_id IN (
            SELECT patient_id FROM Clinical_Visits WHERE systolic_bp > 140
        )
    ''')
    results = cur.fetchall()
    print("\nPatients with Systolic BP > 140:")
    for row in results:
        print(f"Name: {row[0]}, ID: {row[1]}")

def update_sample_location(conn, cur, sample_id=1, new_location='Lab Shelf 2'):
    """Update: Change storage location of one sample."""
    cur.execute('''
        UPDATE Samples
        SET storage_location = ?
        WHERE sample_id = ?
    ''', (new_location, sample_id))
    conn.commit()
    print(f"\nUpdated storage location for Sample ID {sample_id} to '{new_location}'.")

def delete_patient(conn, cur, patient_id=3):
    """Delete: Remove one patient (cascades to visits and samples)."""
    cur.execute('''
        DELETE FROM Patients WHERE patient_id = ?
    ''', (patient_id,))
    conn.commit()
    print(f"\nDeleted Patient ID {patient_id} (and related visits/samples via CASCADE).")

if __name__ == "__main__":
    conn, cur = connect_db()
    try:
        create_tables(cur)
        insert_data(conn, cur)
        
        # Demo reads
        list_patients(cur)  # Simple SELECT
        visits_for_patient(cur, patient_id=1)  # JOIN SELECT
        high_bp_patients(cur)
        
        # Demo update
        update_sample_location(conn, cur, sample_id=1)
        
        # Demo delete
        delete_patient(conn, cur, patient_id=3)
        
        # Verify after delete (re-run a query)
        list_patients(cur)
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        conn.close()