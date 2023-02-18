

use crate::segments::*;
use xgboost::{parameters, DMatrix, Booster};
use crate::*;
use log::{info, warn};
use std::time::Instant;

const N_TRAINING_SAMPLES: usize = 8192 * 2;
const N_FEATURES: usize = 10; 
const N_TRAIN_ITER: u32 = 8;

// const APPROX_AGE_SHIFT: usize = 2;
// const APPROX_SIZE_SHIFT: usize = 2;


pub struct L2Learner {
    pub has_training_data: bool, 
    pub next_train_time: u32, 

    train_x: Vec<f32>,
    train_y: Vec<f32>,
    offline_y: Vec<f32>,
    approx_snapshot_time: Vec<u32>,

    n_obj_since_snapshot: Vec<u16>,
    n_obj_retain_per_seg: Vec<u16>,

    total_train_micros: u128,
    n_training: u32, 

    total_inference_micros: u128,
    n_inference: u32,

    // numbrer of merged segments each merge
    n_merge: u32,

    n_curr_train_samples: usize,

    inference_x: Vec<f32>,

    bst: Option<Booster>, 
}

fn gen_x_from_header(header: &SegmentHeader, base_x: &mut [f32], idx: usize) {

    let start_idx = idx * N_FEATURES;
    let end_idx = start_idx + N_FEATURES;
    let x: &mut [f32] = &mut base_x[start_idx..end_idx];

    x[0] = header.req_rate;
    x[1] = header.write_rate;
    x[2] = header.miss_ratio;
    x[3] = header.live_items as f32;
    x[4] = header.live_bytes as f32;
    x[5] = (CoarseInstant::recent().as_secs() - header.create_at().as_secs()) as f32;
    x[6] = ((header.create_at().as_secs() / 3600) % 24) as f32;
    x[7] = header.n_merge as f32;
    x[8] = header.n_req as f32;
    x[9] = header.n_active as f32;

    // x[0] = quickrandom() as f32;
    // for i in 1..10 {
    //     x[i] = 0.0; 
    // }

    // if header.n_merge > 0 {
    //     x[9] = header.n_req as f32 / (CoarseInstant::recent().as_secs() - header.merge_at().as_secs() + 1) as f32;
    //     x[10]= header.n_active as f32 / (CoarseInstant::recent().as_secs() - header.merge_at().as_secs() + 1) as f32;
    // } else {
    //     x[9] = header.n_req as f32 / (CoarseInstant::recent().as_secs() - header.create_at().as_secs() + 1) as f32;
    //     x[10]= header.n_active as f32 / (CoarseInstant::recent().as_secs() - header.create_at().as_secs() + 1) as f32;
    // }
}


impl L2Learner {
    pub fn new(n_seg: usize, n_merge: u32) -> L2Learner {
        let train_x = vec![0.0; N_TRAINING_SAMPLES * N_FEATURES];
        let train_y = vec![0.0; N_TRAINING_SAMPLES];
        let offline_y = vec![0.0; N_TRAINING_SAMPLES];
        let approx_snapshot_time = vec![0; N_TRAINING_SAMPLES];
        let n_obj_since_snapshot = vec![0; N_TRAINING_SAMPLES];
        let n_obj_retain_per_seg = vec![0; N_TRAINING_SAMPLES];

        L2Learner {
            has_training_data: false,
            next_train_time: 0,

            train_x: train_x,
            train_y: train_y,
            offline_y: offline_y,

            approx_snapshot_time: approx_snapshot_time,
            n_obj_since_snapshot: n_obj_since_snapshot, 
            n_obj_retain_per_seg: n_obj_retain_per_seg,

            total_train_micros: 0, 
            n_training: 0,
            total_inference_micros: 0,
            n_inference: 0,
            
            n_merge: n_merge,
            n_curr_train_samples: 0,

            inference_x: vec![0.0; N_FEATURES * n_seg],

            bst: None,
        }
    }

    // take a snapshot of the segment features to generate train data
    fn snapshot_segment_feature(&mut self, header: &mut SegmentHeader, curr_vtime: u64) -> bool {
        if self.n_curr_train_samples >= N_TRAINING_SAMPLES {
            header.train_data_idx = -1;

            return false;
        }

        header.snapshot_time = CoarseInstant::recent().as_secs() as i32;
        self.n_obj_retain_per_seg[self.n_curr_train_samples] = (header.live_items() / self.n_merge as i32) as u16;
        // self.approx_snapshot_time[self.n_curr_train_samples] = CoarseInstant::recent().as_secs() >> APPROX_AGE_SHIFT;
        self.approx_snapshot_time[self.n_curr_train_samples] = curr_vtime as u32;

        // record the index in the training matrix so that we can update y later
        header.train_data_idx = self.n_curr_train_samples as i32;

        // copy features from header to dense matrix 
        gen_x_from_header(header, &mut self.train_x, self.n_curr_train_samples);

        // set y to 0.0 and will be updated when objects are requested in the future 
        self.train_y[self.n_curr_train_samples] = 0.0;

        // incr the number of training samples 
        self.n_curr_train_samples += 1;        

        return true;
    }

    pub fn gen_training_data(&mut self, headers: &mut [SegmentHeader], curr_vtime: u64) {
        self.n_curr_train_samples = 0;
        // self.n_curr_train_samples /= 2;

        let sample_every_n = std::cmp::max(headers.len() / N_TRAINING_SAMPLES, 1);
        let mut v = sample_every_n; 

        for idx in 0..headers.len() {
            let header = &mut headers[idx];
            v -= 1;
            if v == 0 {
                self.snapshot_segment_feature(header, curr_vtime);
                v = sample_every_n;
            } else {
                header.train_data_idx = -1;
            }
        }

        self.has_training_data = true; 
        info!("{:.2}h generate training data", CoarseInstant::recent().as_secs() as f32 / 3600.0);
    }

    pub fn accu_train_segment_utility(&mut self, idx: i32, size: u32, curr_vtime: u64) {
        if self.n_obj_since_snapshot[idx as usize] < self.n_obj_retain_per_seg[idx as usize] {
            self.n_obj_since_snapshot[idx as usize] += 1;
            return;
        }

        // let approx_age = (CoarseInstant::recent().as_secs() >> APPROX_AGE_SHIFT) - self.approx_snapshot_time[idx as usize] + 1;
        // let approx_size = (size >> APPROX_SIZE_SHIFT) + 1; 
        // let utility = 1.0e4 / approx_age as f32 / approx_size as f32;


        let approx_age = curr_vtime as u32 - self.approx_snapshot_time[idx as usize] + 1;
        // let approx_age = ((curr_vtime as u32 - self.approx_snapshot_time[idx as usize] + 1) as f32).log2() + 1.0;
        let approx_size = size; 
        let utility = 1.0e8 / approx_age as f32 / approx_size as f32;

        assert!(utility < f32::MAX / 2.0, "{} {} {}", approx_age, approx_size, curr_vtime as u32 - self.approx_snapshot_time[idx as usize]);
        self.train_y[idx as usize] += utility;
    }

    pub fn set_train_segment_utility(&mut self, idx: i32, utility: f32) {
        self.train_y[idx as usize] = utility;
    }

    pub fn set_offline_segment_utility(&mut self, idx: i32, utility: f32) {
        self.offline_y[idx as usize] = utility;
    }

    #[allow(dead_code)]
    fn normalize_train_y(&mut self) {
        // normalize training data Y 
        let max_y = *self.train_y.iter().max_by(|x, y| x.partial_cmp(&y).unwrap()).unwrap();
        let min_y = *self.train_y.iter().max_by(|x, y| y.partial_cmp(&x).unwrap()).unwrap();

        if max_y - min_y < 1e-8 {
            warn!("[WARN] max_y ({}) - min_y ({}) < 1e-8", max_y, min_y);
            return;
        }

        for i in 0..self.n_curr_train_samples {
            self.train_y[i] = (self.train_y[i] - min_y) / (max_y - min_y);
        }
    }

    #[allow(dead_code)]
    fn cap_train_y(&mut self) {
        let mut sorted_train_y = self.train_y.to_vec(); 
        sorted_train_y.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let y_cap = sorted_train_y[(sorted_train_y.len() as usize) * 9 / 10 - 1];
        // println!("{:?}", sorted_train_y); 
        // println!("{:?}", y_cap);

        for i in 0..self.n_curr_train_samples {
            if self.train_y[i] > y_cap {
                self.train_y[i] = y_cap;
            }
        }
    }

    pub fn train(&mut self) {
        if !self.has_training_data {
            return;
        }
        
        let start_time = Instant::now();


        info!("{:.2}h train {} samples", 
                    CoarseInstant::recent().as_secs() as f32 / 3600.0, self.n_curr_train_samples, ); 

        // println!("        y    offline_y     req_rate  write_rate  miss  n_item live_byte  age    create_hour   merge  req    active");  
        // for i in 0..20 {
        //     println!("{:12.4} {:12.4} [{:8.2} {:8.2} {:8.2} {:8.0} {:8.0} {:8.0} {:8.0} {:8.0} {:6.0} {:6.0}]", 
        //         self.train_y[i], self.offline_y[i], 
        //         self.train_x[i*N_FEATURES], 
        //         self.train_x[i*N_FEATURES + 1], 
        //         self.train_x[i*N_FEATURES + 2], 
        //         self.train_x[i*N_FEATURES + 3], 
        //         self.train_x[i*N_FEATURES + 4], 
        //         self.train_x[i*N_FEATURES + 5], 
        //         self.train_x[i*N_FEATURES + 6], 
        //         self.train_x[i*N_FEATURES + 7], 
        //         self.train_x[i*N_FEATURES + 8], 
        //         self.train_x[i*N_FEATURES + 9], 
        //     );
        // }

        // self.normalize_train_y();
        // self.cap_train_y(); 

        // convert train data into XGBoost's matrix format
        let n_row = self.n_curr_train_samples as usize / 10 * 9;
        let mut dtrain = DMatrix::from_dense(&self.train_x[.. n_row * N_FEATURES], n_row).unwrap();
        dtrain.set_labels(&self.train_y[..n_row]).unwrap();
        
        // validation data
        let n_row_valid = self.n_curr_train_samples as usize - n_row; 
        let start = n_row * N_FEATURES;
        let end = (n_row + n_row_valid) * N_FEATURES;
        let mut dvalid = DMatrix::from_dense(&self.train_x[start..end], n_row_valid).unwrap();
        dvalid.set_labels(&self.train_y[n_row..n_row+n_row_valid]).unwrap();

        // configure objectives, metrics, etc.
        let learning_params = parameters::learning::LearningTaskParametersBuilder::default()
            .objective(parameters::learning::Objective::RegSquareError)
            .build().unwrap();

        // configure the tree-based learning model's parameters
        let tree_params = parameters::tree::TreeBoosterParametersBuilder::default()
                // .max_depth(32)
                // .eta(1.0)
                .build().unwrap();

        // overall configuration for Booster
        let booster_params = parameters::BoosterParametersBuilder::default()
            .booster_type(parameters::BoosterType::Tree(tree_params))
            .learning_params(learning_params)
            .verbose(false)
            .build().unwrap();

        // specify datasets to evaluate against during train
        let evaluation_sets = [(&dtrain, "train"), (&dvalid, "valid"),];

        // overall configuration for train/evaluation
        let params = parameters::TrainingParametersBuilder::default()
            .dtrain(&dtrain)                         // dataset to train with
            .boost_rounds(N_TRAIN_ITER)              // number of train iterations
            .booster_params(booster_params)          // model parameters
            .evaluation_sets(Some(&evaluation_sets))  // optional datasets to evaluate against in each iteration
            .build().unwrap();
        
        // train model, and print evaluation data
        self.bst = Some(Booster::train(&params).unwrap());

        let elapsed = start_time.elapsed().as_micros();
        self.total_train_micros += elapsed;
        self.n_training += 1; 
        info!("{} training {} micros per training, {} inference {} micros", 
            self.n_training, 
            self.total_train_micros / self.n_training as u128,
            self.n_inference,
            self.total_inference_micros / std::cmp::max(self.n_inference, 1) as u128,
        ); 

        // save model to file
        // self.bst.save("xgb.model").unwrap();
        // self.bst = Booster::load("xgb.model").unwrap();

        // save data 
        // dtrain.save("dtrain.dmat").unwrap();
        // let dtrain = DMatrix::load("dtrain.dmat").unwrap();
    }


    pub fn inference(&mut self, headers: &mut [SegmentHeader]) {
        let start_time = Instant::now();

        // copy from headers to inference_x
        for idx in 0..headers.len() {
            gen_x_from_header(&headers[idx], &mut self.inference_x, idx);
        }
        let inf_dmatrix = DMatrix::from_dense(&self.inference_x, headers.len()).unwrap(); 

        // predict
        let preds = self.bst.as_ref().unwrap().predict(&inf_dmatrix).unwrap();

        // copy predicted segment utility to headers 
        for (idx, pred_utility) in preds.iter().enumerate() {
            if headers[idx].next_seg().is_none() {
                headers[idx].pred_utility = 1.0e8;
            } else {
                headers[idx].pred_utility = *pred_utility;
            }
        }

        self.n_inference += 1;
        let elapsed = start_time.elapsed().as_micros();
        self.total_inference_micros += elapsed;
    }
}


