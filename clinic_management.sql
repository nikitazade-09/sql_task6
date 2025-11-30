-- Query 1: Create a new database
CREATE DATABASE Clinic_Management_DB;

-- Connect to the new database (Clinic_Management_DB)

-- Query 2: Create Doctors table (8 columns)
CREATE TABLE Doctors (
    doctor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    specialty VARCHAR(100) NOT NULL,
    phone_number VARCHAR(15),
    email VARCHAR(100) UNIQUE,
    shift_start TIME NOT NULL,
    shift_end TIME NOT NULL
);

-- Query 3: Create Appointments table (8 columns)
CREATE TABLE Appointments (
    appointment_id SERIAL PRIMARY KEY,
    patient_name VARCHAR(100) NOT NULL,
    doctor_id INT NOT NULL REFERENCES Doctors(doctor_id),
    appointment_time TIMESTAMP NOT NULL,
    reason_for_visit VARCHAR(200),
    status VARCHAR(20) DEFAULT 'Scheduled', -- Scheduled, Cancelled, Completed
    duration_minutes INT DEFAULT 30,
    clinic_room VARCHAR(10)
);

-- Query 4: Function to insert data into the Doctors table with 6+ checks
CREATE OR REPLACE FUNCTION add_new_doctor (
    p_first_name VARCHAR,
    p_last_name VARCHAR,
    p_specialty VARCHAR,
    p_phone_number VARCHAR,
    p_email VARCHAR,
    p_shift_start TIME,
    p_shift_end TIME
)
RETURNS VARCHAR AS $$
DECLARE
    error_message VARCHAR := '';
    valid_specialties TEXT[] := ARRAY['Cardiology', 'Pediatrics', 'Oncology', 'Dermatology', 'General Practice'];
BEGIN
    -- CONDITION 1: Check if shift start time is before shift end time
    IF p_shift_start >= p_shift_end THEN
        error_message := error_message || 'Shift Start time must be before Shift End time. ';
    END IF;

    -- CONDITION 2: Check for a valid specialty
    IF NOT (p_specialty = ANY(valid_specialties)) THEN
        error_message := error_message || 'Specialty is invalid. Must be one of: ' || array_to_string(valid_specialties, ', ') || '. ';
    END IF;

    -- CONDITION 3: Check for empty name fields
    IF p_first_name = '' OR p_last_name = '' THEN
        error_message := error_message || 'First and Last Names cannot be empty. ';
    END IF;

    -- CONDITION 4: Check phone number length (e.g., must be 10 characters if assuming a standard format)
    IF LENGTH(REPLACE(p_phone_number, '-', '')) != 10 THEN
        error_message := error_message || 'Phone number must contain 10 digits. ';
    END IF;

    -- CONDITION 5: Check for duplicate email (Unique constraint pre-check)
    IF EXISTS (SELECT 1 FROM Doctors WHERE email = p_email) THEN
        error_message := error_message || 'An account with this email already exists. ';
    END IF;

    -- CONDITION 6: Check for minimum shift length (e.g., must be at least 4 hours)
    IF (p_shift_end - p_shift_start) < INTERVAL '4 hour' THEN
        error_message := error_message || 'Minimum shift duration is 4 hours. ';
    END IF;

    -- If any error message exists, return it
    IF error_message != '' THEN
        RETURN 'Doctor Insertion FAILED. Errors: ' || TRIM(error_message);
    ELSE
        -- If all checks pass, perform the insertion
        INSERT INTO Doctors (first_name, last_name, specialty, phone_number, email, shift_start, shift_end)
        VALUES (p_first_name, p_last_name, p_specialty, p_phone_number, p_email, p_shift_start, p_shift_end);
        RETURN 'Doctor Insertion SUCCESSFUL for: ' || p_last_name;
    END IF;

END;
$$ LANGUAGE plpgsql;

-- Query 5: Function to insert data into the Appointments table with 6+ checks
CREATE OR REPLACE FUNCTION schedule_appointment (
    p_patient_name VARCHAR,
    p_doctor_id INT,
    p_appointment_time TIMESTAMP,
    p_reason_for_visit VARCHAR,
    p_duration_minutes INT
)
RETURNS VARCHAR AS $$
DECLARE
    error_message VARCHAR := '';
    doctor_shift_start TIME;
    doctor_shift_end TIME;
    appt_date DATE := p_appointment_time::DATE;
    appt_time TIME := p_appointment_time::TIME;
BEGIN
    -- Get doctor's shift times
    SELECT shift_start, shift_end INTO doctor_shift_start, doctor_shift_end FROM Doctors WHERE doctor_id = p_doctor_id;

    -- CONDITION 1: Check if the doctor ID is valid/exists
    IF NOT FOUND THEN
        error_message := error_message || 'Doctor ID ' || p_doctor_id || ' does not exist. ';
    END IF;

    -- CONDITION 2: Check if appointment is at least 30 minutes in the future
    IF p_appointment_time <= NOW() + INTERVAL '30 minutes' THEN
        error_message := error_message || 'Appointment must be scheduled at least 30 minutes in advance. ';
    END IF;

    -- CONDITION 3: Check if the appointment duration is valid (e.g., must be > 0 and a multiple of 15)
    IF p_duration_minutes <= 0 OR p_duration_minutes % 15 != 0 THEN
        error_message := error_message || 'Appointment duration must be positive and a multiple of 15 minutes. ';
    END IF;

    -- CONDITION 4: Check if the appointment is within the doctor's shift
    IF appt_time < doctor_shift_start OR (appt_time + (p_duration_minutes || ' minutes')::INTERVAL) > doctor_shift_end THEN
        error_message := error_message || 'Appointment time is outside of Doctor ' || p_doctor_id || '''s shift (' || doctor_shift_start || ' to ' || doctor_shift_end || '). ';
    END IF;

    -- CONDITION 5: Check for scheduling conflict (Overlap check for the same doctor)
    IF EXISTS (
        SELECT 1 FROM Appointments
        WHERE doctor_id = p_doctor_id
        AND status = 'Scheduled'
        AND appointment_time::DATE = appt_date
        -- Checks for time overlap
        AND (p_appointment_time, p_appointment_time + (p_duration_minutes || ' minutes')::INTERVAL) OVERLAPS (appointment_time, appointment_time + (duration_minutes || ' minutes')::INTERVAL)
    ) THEN
        error_message := error_message || 'Scheduling conflict: Doctor is already booked at this time. ';
    END IF;

    -- CONDITION 6: Check for a reason for the visit
    IF TRIM(p_reason_for_visit) = '' THEN
        error_message := error_message || 'Reason for visit cannot be empty. ';
    END IF;

    -- If any error message exists, return it
    IF error_message != '' THEN
        RETURN 'Appointment Scheduling FAILED. Errors: ' || TRIM(error_message);
    ELSE
        -- If all checks pass, perform the insertion
        INSERT INTO Appointments (patient_name, doctor_id, appointment_time, reason_for_visit, duration_minutes)
        VALUES (p_patient_name, p_doctor_id, p_appointment_time, p_reason_for_visit, p_duration_minutes);
        RETURN 'Appointment SUCCESSFUL for ' || p_patient_name || ' with Doctor ' || p_doctor_id;
    END IF;

END;
$$ LANGUAGE plpgsql;



-- Query 6: Function for aggregation (Total Scheduled Time by Specialty)
CREATE OR REPLACE FUNCTION get_weekly_specialty_utilization (
    p_specialty VARCHAR
)
RETURNS INT AS $$
DECLARE
    total_minutes INT := 0;
BEGIN
    -- Use SUM() to aggregate the duration_minutes for all appointments
    SELECT
        SUM(A.duration_minutes) INTO total_minutes
    FROM
        Appointments A
    JOIN
        Doctors D ON A.doctor_id = D.doctor_id
    WHERE
        D.specialty = p_specialty
        -- Filter for appointments in the current week (from Monday to Sunday)
        AND A.appointment_time >= date_trunc('week', CURRENT_DATE)
        AND A.appointment_time < date_trunc('week', CURRENT_DATE) + INTERVAL '1 week'
        AND A.status = 'Scheduled';

    -- Return the total calculated minutes, defaulting to 0 if NULL
    RETURN COALESCE(total_minutes, 0);
END;
$$ LANGUAGE plpgsql;


-- Query 6 (Revised): Function for aggregation using CASE statement
CREATE OR REPLACE FUNCTION get_weekly_specialty_performance (
    p_specialty VARCHAR
)
RETURNS TABLE (
    completed_appointments BIGINT,
    cancelled_appointments BIGINT
) AS $$
BEGIN
    -- This single query uses conditional aggregation (SUM combined with CASE)
    -- to count rows based on their 'status' column.
    RETURN QUERY
    SELECT
        -- Count all appointments where status is 'Completed'
        SUM(CASE WHEN A.status = 'Completed' THEN 1 ELSE 0 END) AS completed_count,
        -- Count all appointments where status is 'Cancelled'
        SUM(CASE WHEN A.status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_count
    FROM
        Appointments A
    JOIN
        Doctors D ON A.doctor_id = D.doctor_id
    WHERE
        D.specialty = p_specialty
        -- Filter for appointments in the current week
        AND A.appointment_time >= date_trunc('week', CURRENT_DATE)
        AND A.appointment_time < date_trunc('week', CURRENT_DATE) + INTERVAL '1 week';
END;
$$ LANGUAGE plpgsql;


-- Example: Get the weekly performance for Pediatrics
-- This will return a single row with two columns: completed_appointments and cancelled_appointments
SELECT * FROM get_weekly_specialty_performance('Pediatrics');



-- Insert Doctor 1: Jane Smith (Pediatrics)
SELECT add_new_doctor('Jane', 'Smith', 'Pediatrics', '5551234567', 'jsmith@clinic.com', '08:00:00', '17:00:00');

-- Insert Doctor 2: Alex Lee (Cardiology)
SELECT add_new_doctor('Alex', 'Lee', 'Cardiology', '5559876543', 'alee@clinic.com', '09:00:00', '18:00:00');

-- Insert Doctor 3: Chris Evans (General Practice)
SELECT add_new_doctor('Chris', 'Evans', 'General Practice', '5550001111', 'cevans@clinic.com', '10:00:00', '14:00:00');


-- Appointment 1: For Doctor 1 (Jane Smith - Pediatrics)
SELECT schedule_appointment(
    'Billy Jones'::VARCHAR,
    1,
    (NOW() + interval '2 hour')::TIMESTAMP, -- Casting to TIMESTAMP (WITHOUT TIME ZONE)
    'Routine check-up for cough'::VARCHAR,
    30
);

-- Appointment 2: For Doctor 2 (Alex Lee - Cardiology)
SELECT schedule_appointment(
    'Sarah Connor'::VARCHAR,
    2,
    (NOW() + interval '4 hour')::TIMESTAMP, -- Casting to TIMESTAMP (WITHOUT TIME ZONE)
    'Follow-up for blood pressure'::VARCHAR,
    45
);

-- Appointment 3: For Doctor 1 (Jane Smith - Pediatrics) - Mark as completed
SELECT schedule_appointment(
    'Mike Davis'::VARCHAR,
    1,
    (NOW() + interval '6 hour')::TIMESTAMP, -- Casting to TIMESTAMP (WITHOUT TIME ZONE)
    'Immunization shot'::VARCHAR,
    15
);

-- Update one of the appointments to 'Completed' for the aggregation function test
UPDATE Appointments SET status = 'Completed' WHERE patient_name = 'Mike Davis';

SELECT doctor_id, last_name FROM Doctors ORDER BY doctor_id;

SELECT
    *
FROM
    Doctors;

SELECT
    *
FROM
    Appointments
ORDER BY
    appointment_time DESC;	

SELECT doctor_id, first_name, last_name, specialty FROM Doctors;	

SELECT schedule_appointment('Test Patient', 999, NOW() + interval '1 hour', 'Test', 30);

CREATE OR REPLACE FUNCTION insert_20_doctors()
RETURNS VOID AS $$
DECLARE
    i INT;
    f_name VARCHAR;
    l_name VARCHAR;
    specialty_list TEXT[] := ARRAY['Cardiology', 'Pediatrics', 'Oncology', 'Dermatology', 'General Practice'];
    current_specialty VARCHAR;
    start_time TIME;
    end_time TIME;
BEGIN
    FOR i IN 1..20 LOOP
        -- Generate unique names
        f_name := 'Doctor' || i;
        l_name := 'Test' || (i * 2);

        -- Cycle through the 5 specialties
        current_specialty := specialty_list[(i - 1) % 5 + 1];

        -- Create staggered shift times (e.g., 08:00 to 16:00, 09:00 to 17:00)
        start_time := ('08:00'::TIME + (i - 1) * INTERVAL '1 hour') % INTERVAL '24 hour' + '08:00'::TIME;
        end_time := start_time + INTERVAL '8 hour'; -- Ensure shift is at least 4 hours (Condition 6)

        -- Call the existing function to insert data with checks
        PERFORM add_new_doctor(
            f_name,
            l_name,
            current_specialty,
            '555' || LPAD(i::text, 7, '0'), -- Unique phone number
            LOWER(f_name) || i || '@clinic.com', -- Unique email
            start_time,
            end_time
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_20_appointments()
RETURNS VOID AS $$
DECLARE
    i INT;
    p_name VARCHAR;
    random_doctor_id INT;
    appt_time TIMESTAMP;
    min_doc_id INT := 1;
    max_doc_id INT := 20; -- Based on the 20 doctors we inserted
    -- Note: Ensure your Doctors table has IDs up to 20 before running this!
BEGIN
    FOR i IN 1..20 LOOP
        -- Generate unique patient name
        p_name := 'Patient' || i;

        -- Select a random doctor ID between 1 and 20
        random_doctor_id := FLOOR(RANDOM() * (max_doc_id - min_doc_id + 1) + min_doc_id)::INT;

        -- Set appointment time in the near future (Condition 2 check)
        appt_time := (NOW() + (i * INTERVAL '1 hour'))::TIMESTAMP;

        -- Call the existing function to insert data with checks
        PERFORM schedule_appointment(
            p_name::VARCHAR,
            random_doctor_id,
            appt_time,
            'Reason: Symptoms check ' || (i % 3 + 1)::VARCHAR, -- Cycle through a few reasons
            30
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT insert_20_appointments();
SELECT COUNT(*) AS total_doctors FROM Doctors;


SELECT COUNT(*) AS total_appointments FROM Appointments;

-- View all 20 rows and 8 columns of the Doctors table
SELECT * FROM Doctors;

-- View all 20 rows and 8 columns of the Appointments table
SELECT * FROM Appointments;

-- 1. Disable Foreign Key Checks temporarily
SET session_replication_role = 'replica';

-- 2. Clear all data
TRUNCATE TABLE Appointments RESTART IDENTITY CASCADE;
TRUNCATE TABLE Doctors RESTART IDENTITY CASCADE;

-- 3. Re-enable Foreign Key Checks
SET session_replication_role = 'origin';

-- 4. Verify tables are empty (Count should be 0)
SELECT COUNT(*) FROM Doctors;
SELECT COUNT(*) FROM Appointments;



CREATE OR REPLACE FUNCTION insert_20_doctors()
RETURNS VOID AS $$
DECLARE
    i INT;
    f_name VARCHAR;
    l_name VARCHAR;
    specialty_list TEXT[] := ARRAY['Cardiology', 'Pediatrics', 'Oncology', 'Dermatology', 'General Practice'];
    current_specialty VARCHAR;
BEGIN
    FOR i IN 1..20 LOOP
        f_name := 'Doctor' || i;
        l_name := 'Test' || (i * 2);
        current_specialty := specialty_list[(i - 1) % 5 + 1];

        -- Use static, valid shift times (e.g., 8am to 4pm) to ensure Condition 1 and 6 always pass
        PERFORM add_new_doctor(
            f_name,
            l_name,
            current_specialty,
            '555' || LPAD(i::text, 7, '0'),
            LOWER(f_name) || i || '@clinic.com',
            '08:00:00'::TIME, -- Safe Start Time
            '16:00:00'::TIME  -- Safe End Time (8 hours duration)
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute the corrected doctor insertion
SELECT insert_20_doctors();

-- Verify 20 rows
SELECT COUNT(*) AS total_doctors FROM Doctors;

CREATE OR REPLACE FUNCTION insert_20_appointments()
RETURNS VOID AS $$
DECLARE
    i INT;
    p_name VARCHAR;
    random_doctor_id INT;
    appt_time TIMESTAMP;
BEGIN
    FOR i IN 1..20 LOOP
        p_name := 'Patient' || i;

        -- Ensure doctor_id is between 1 and 20
        random_doctor_id := (i - 1) % 20 + 1; -- Cycles through 1 to 20 sequentially

        -- Set appointment time safely in the future (starting 2 days from now)
        appt_time := (CURRENT_DATE + INTERVAL '2 day' + (i * INTERVAL '1 hour'))::TIMESTAMP;

        -- Call the existing function with explicit casts
        PERFORM schedule_appointment(
            p_name::VARCHAR,
            random_doctor_id,
            appt_time,
            'Reason: Consultation for Patient ' || i::VARCHAR,
            30
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Execute the corrected appointment insertion
SELECT insert_20_appointments();

-- Verify 20 rows
SELECT COUNT(*) AS total_appointments FROM Appointments;

SELECT add_new_doctor(
    'Valid', 'Doctor', 'Pediatrics', '5551112222', 'valid@clinic.com', '08:00:00', '16:00:00'
);

SELECT add_new_doctor(
    '', -- Empty name (Condition 3 fails)
    'Invalid',
    'Psychology', -- Invalid specialty (Condition 2 fails)
    '123',        -- Short phone number (Condition 4 fails)
    'invalid@test.com',
    '10:00:00',
    '11:00:00'    -- Short shift (Condition 6 fails)
);


